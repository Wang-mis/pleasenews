import paramiko
import os

import requests
from tqdm import tqdm


def upload_file(local_path: str, remote_path: str, hostname: str, username: str, password: str, port=22) -> bool:
    sftp, transport = None, None
    try:
        # 连接
        transport = paramiko.Transport((hostname, port))
        transport.connect(username=username, password=password)
        sftp = paramiko.SFTPClient.from_transport(transport)

        # 创建文件夹
        remote_dir = os.path.dirname(remote_path)
        dirs = []
        current_dir = remote_dir
        while len(current_dir) > 1:
            dirs.append(current_dir)
            current_dir = os.path.dirname(current_dir)
        dirs.reverse()
        for d in dirs:
            try:
                sftp.stat(d)  # 尝试检查目录是否存在
            except FileNotFoundError:
                sftp.mkdir(d)  # 如果目录不存在，则创建
                print(f"文件夹不存在，创建文件夹：{d}")
            except Exception as e:
                print(f"\033[31m在远程服务器上创建文件夹 {d} 发生错误：{e}\033[0m")
                return False

        # 获取文件大小
        file_size = os.path.getsize(local_path)

        # 传输文件
        with open(local_path, 'rb') as file, tqdm(total=file_size, unit='B', unit_scale=True, desc=remote_path) as pbar:
            def progress_callback(transferred, total):
                pbar.update(transferred - pbar.n)

            sftp.put(local_path, remote_path, callback=progress_callback)
        return True
    except Exception as e:
        print(f"\033[31m传输过程中发生错误: {e}\033[0m")
        return False
    finally:
        if sftp is not None:
            sftp.close()
        if transport is not None:
            transport.close()


def upload_data_day(day):
    media_merge_path = "merge/" + day + ".media.merge.csv"
    mention_path = "pnews/" + day + "/MentionSourceNames.csv"
    keywords_path = "pnews/" + day + "/Keywords_check.csv"
    # 上传数据文件
    upload_file(
        media_merge_path,
        '/home/wsx/remote/pleasenews/' + media_merge_path,
        '123.57.216.53',
        'root',
        'lincvis1024..'
    )
    upload_file(
        mention_path,
        '/home/wsx/remote/pleasenews/' + mention_path,
        '123.57.216.53',
        'root',
        'lincvis1024..'
    )
    upload_file(
        keywords_path,
        '/home/wsx/remote/pleasenews/' + keywords_path,
        '123.57.216.53',
        'root',
        'lincvis1024..'
    )
    # 通知后端服务器更新数据
    response = requests.get("http://123.57.216.53:14451/dataset/update_day", json={"day": day},
                            headers={'Content-Type': 'application/json'})
    if response.status_code == 200:
        print("已通知后端服务器更新数据。")
    else:
        print("\033[31m通知后端服务器更新数据失败！\033[0m")


# 测试
if __name__ == '__main__':
    # upload_file(
    #     r'D:\Programs\github\pleasenews\helper\SQLiteTest.db',
    #     '/home/wsx/remote/pleasenews/helper/SQLiteTest1.db',
    #     '123.57.216.53',
    #     'root',
    #     'lincvis1024..'
    # )

    upload_data_day('20241007')
