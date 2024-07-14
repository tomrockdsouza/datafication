import webview
from flask import Flask, Response, request, abort
import wx
from threading import Thread
from tendo import singleton
import socket
import pandas as pd
from pathlib import Path

app = Flask(__name__)
wx_app = wx.App(False)


def is_port_in_use(port, host='127.0.0.1'):
    """Check if the specified port is in use on the given host."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.settimeout(1)  # 1 second timeout for quick checks
        try:
            sock.connect((host, port))
        except socket.error:
            return False
        return True


def select_port():
    for port_no in range(1025, 65536):
        if not is_port_in_use(port_no):
            return port_no


@app.before_request
def limit_remote_addr():
    if request.remote_addr != '127.0.0.1':
        abort(403)


def on_save_file(specific_wildcard):
    wildcard = f"{specific_wildcard}|All files (*.*)|*.*"
    dialog = wx.FileDialog(
        None, "Save File As", wildcard=wildcard,
        style=wx.FD_SAVE | wx.FD_OVERWRITE_PROMPT
    )
    save_path = None
    if dialog.ShowModal() == wx.ID_OK:
        save_path = dialog.GetPath()
    dialog.Destroy()
    return save_path


def choose_many_files():
    wildcard = "All Files (*.*)|*.*"
    dialog = wx.FileDialog(
        None, "Select Files", wildcard=wildcard,
        style=wx.FD_OPEN | wx.FD_MULTIPLE | wx.FD_FILE_MUST_EXIST
    )
    paths = []
    if dialog.ShowModal() == wx.ID_OK:
        paths = dialog.GetPaths()
    dialog.Destroy()
    return paths


def convert_many_parquet_to_xlsx():
    paths = choose_many_files()
    if len(paths)==0:
        return '<p style="background-color:#FFFACD">No parquet file selected.</p>'
    xlsx_file_name = on_save_file('Excel files (*.xlsx)|*.xlsx')
    if not xlsx_file_name:
        return '<p style="background-color:#FFFACD">User aborted operation.</p>'
    dictx = {}
    errors_list = []
    for path in paths:
        try:
            dataframe = pd.read_parquet(path, engine='pyarrow')
            dictx[Path(path).stem] = dataframe
        except:
            print(1)
            errors_list.append(
                f'<p style="background-color:pink">{Path(path).name} is not a valid parquet file.</p>'
            )
    if len(dictx) == 0:
        return (''.join(errors_list)
                +'<p style="background-color:#FFFACD">No valid parquet files found.</p>')
    try:
        with pd.ExcelWriter(xlsx_file_name, engine='openpyxl') as writer:
            for sheetname, dataframe in dictx.items():
                dataframe.to_excel(writer, sheet_name=sheetname, index=False)
        return (
            ''.join(errors_list)
            +f'<p style="background-color:#98FB98">Success! Data saved at<br>{xlsx_file_name}</p>'
        )
    except PermissionError:
        return (
            ''.join(errors_list)
            +f'<p style="background-color:pink">{Path(xlsx_file_name).name} is being used by another program.</p>'
        )


@app.route('/', methods=['GET'])
def appx():
    query_params = request.args
    mode = query_params.get('mode', None)
    if mode=='MANY_PARQUET_TO_ONE_XLSX':
        conversion_response = convert_many_parquet_to_xlsx()
    else:
        conversion_response=''
    html_output=f'''
    <div style="width:300px;word-break: break-all;">
        <a href='/?mode=MANY_PARQUET_TO_ONE_XLSX' onclick='this.remove()' >Convert many parquet files into one xlsx.<br></a>
        {'<p><u>Previous Operation Log</u></p>'+conversion_response if conversion_response else ''}
    </div>
    '''
    return Response(html_output, content_type='text/html')


if __name__ == '__main__':
    try:
        me = singleton.SingleInstance()
    except:
        exit()
    port_no = select_port()
    import os

    print(os.getcwd())
    # Run Flask in a separate thread
    thread = Thread(target=lambda: app.run(port=port_no, use_reloader=False))
    thread.daemon = True
    thread.start()

    # Create a webview window
    main_window = webview.create_window(
        title='Datafication',
        url=f'http://127.0.0.1:{port_no}',
        height=200,
        width=350
    )
    webview.start()
