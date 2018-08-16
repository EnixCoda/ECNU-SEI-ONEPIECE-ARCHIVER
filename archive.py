# -*- coding: utf-8 -*-
import json
import sys
import os
import multiprocessing
import urllib
import urllib2
import zipfile
import datetime

from qiniu import Auth
from qiniu import BucketManager


def load_config():
    if not os.path.exists("bucket.json"):
        return None
    with open("bucket.json", "r") as bucket_config_file:
        uni_config = json.load(bucket_config_file)
        config = {}
        for uni_key, uni_val in uni_config.items():
            config[uni_key.encode("utf-8")] = uni_val.encode("utf-8")
        return config


def multi_thread_work(func, arr, processes):
    pool = multiprocessing.Pool(processes=processes)
    outputs = pool.map(func, arr)
    pool.close()
    pool.join()
    return outputs


def download_from_qiniu(key):
    try:
        content = urllib2.urlopen(CONFIG["BUCKET_DOMAIN"] + urllib.quote(key)).read()
        if not content:
            return
    except Exception, e:
        print "could not download", key
        return

    key = CONFIG["STORAGE_ROOT"] + key
    path = key.split("/")
    filename = path.pop()
    i = 1
    while i <= len(path):
        tmp_path = "/".join(path[0:i])
        if os.path.exists(tmp_path):
            if not os.path.isdir(tmp_path):
                rmdir(tmp_path)
                os.mkdir(tmp_path)
        else:
            os.mkdir(tmp_path)
        i += 1
    with open(key, "wb") as targetFile:
        targetFile.write(content)
        return True


def rmdir(path):
    for item in os.listdir(path):
        if os.path.isfile(path + item):
            os.remove(path + item)
        else:
            if os.path.isdir(path + item):
                rmdir(path + item)
            else:
                return
    os.rmdir(path)


def move_local_file(ori, new):
    # TODO
    return


def remove_local_file(key):
    os.remove(CONFIG["STORAGE_ROOT"] + key)


def zip_dir(path, name):
    zip_file_name = CONFIG["ARCHIVE_DIR"] + name + ".zip"
    zipf = zipfile.ZipFile(zip_file_name, "w", zipfile.ZIP_DEFLATED)
    zipdir(path, zipf)
    zipf.close()
    return


def zipdir(path, zip_handler):
    for item in os.listdir(path):
        if os.path.isdir(path + "/" + item):
            zipdir(path + "/" + item, zip_handler)
        if os.path.isfile(path + "/" + item) and item != ".DS_Store":
            zip_handler.write(os.path.join(path, item), "/".join((path + "/" + item).split("/")[2:]))


def zip_local_files():
    if not os.path.isdir(CONFIG["ARCHIVE_DIR"]):
        os.mkdir(CONFIG["ARCHIVE_DIR"])
    for lessonClass in os.listdir(CONFIG["STORAGE_ROOT"]):
        if os.path.isdir(CONFIG["STORAGE_ROOT"] + lessonClass):
            for lesson in os.listdir(CONFIG["STORAGE_ROOT"] + lessonClass):
                if os.path.isdir(CONFIG["STORAGE_ROOT"] + lessonClass + "/" + lesson):
                    zip_dir(CONFIG["STORAGE_ROOT"] + lessonClass + "/" + lesson, lesson)


def get_remote_category():

    def filter_file(file_item):
        file_key = file_item["key"]
        return file_key.find(u"__ARCHIVE__") == -1 \
            and file_key.find(u"_log") == -1 \
            and file_key.find(u"申请-") == -1 \
            and file_key.find(u".DS_Store") == -1

    auth = Auth(CONFIG["AK"], CONFIG["SK"])
    bucket = BucketManager(auth)
    files = []
    marker = None
    while True:
        response, eof, info = bucket.list(CONFIG["BUCKET_NAME"], None, marker, 1000, None)
        if info.status_code == 200:
            files = files + response["items"]
            if eof:
                break
            if "marker" in response:
                marker = response["marker"]
        else:
            return False

    category_remote = {}
    for file_list_item in filter(filter_file, files):
        size = file_list_item["fsize"]
        file_id = file_list_item["hash"]
        key = file_list_item["key"]
        time = file_list_item["putTime"]
        category_remote[file_id] = {
            "key": key, "size": size, "time": time
        }
    return category_remote


def get_local_category(category_file):
    if not os.path.exists(category_file):
        with open(category_file, "w") as categoryFile:
            categoryFile.write("{}")
    with open(category_file, "r") as categoryFile:
        category = json.load(categoryFile)
    return category


def compare_local_and_remote(category_local, category_remote):
    tasks = []
    for file_id in category_local:
        # totally removed from remote storage
        if file_id not in category_remote:
            remove_local_file(category_local[file_id]["key"])
            continue
        # moved in remote storage
        remote_key = category_remote[file_id]["key"].encode("utf-8")
        if category_local[file_id]["key"] != remote_key.decode("utf-8"):
            move_local_file(category_local[file_id]["key"], remote_key)
            continue
        # no local copy
        if not os.path.exists(CONFIG["STORAGE_ROOT"] + remote_key):
            tasks.append(remote_key)
    for file_id in category_remote:
        # new in remote storage
        if file_id not in category_local:
            remote_key = category_remote[file_id]["key"].encode("utf-8")
            if not os.path.exists(CONFIG["STORAGE_ROOT"] + remote_key):
                tasks.append(remote_key)
    for task in tasks:
        print task
    return tasks


def main():
    print datetime.datetime.now().strftime("%c")
    if CONFIG is not None:
        if len(sys.argv) == 2:
            category_file = sys.argv[1]
        else:
            category_file = "category.json"
    else:
        print "config file not found"
        return

    print "retrieving remote file list"
    # category = {
    #   fileId:  [key, size, time],
    #   ...
    # }
    category_local = get_local_category(category_file)
    category_remote = get_remote_category()
    if not category_remote:
        print "error occurred retrieving list"
        return

    print "retrieved file list, comparing"
    tasks = compare_local_and_remote(category_local, category_remote)
    print len(tasks), "files to download"
    with open(category_file, "w") as categoryFile:
        json.dump(category_remote, categoryFile)

    if len(tasks) > 0:
        output = multi_thread_work(download_from_qiniu, tasks, 8)
        print "files downloaded, zipping"
        zip_local_files()
        print "done"


CONFIG = load_config()
if __name__ == "__main__":
    print '*' * 40
    main()
