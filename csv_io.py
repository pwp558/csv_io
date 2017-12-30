
# -*- coding: utf-8 -*-

# This class uses multi_processing to speed up the IO process for large csv files
# process_num: how many processes going to use
# def load_csv(self, file_name):   Load csv file to pandas dataframe
# def save_to_csv(self, df, file_name):  save dataframe to disk

# ================================== load csv =====================================
#     The load csv function actually does NOT save much time!!
#     because lots of time need for merging all dataset into one dataset
#     and also the time needed for split the big csv to small csvs is a lot
#     Over all, for a 19G csv, if load by pandas directly need about 400 seconds
#     with this method may need 350 seconds
#     If no need for merge small dataset back to 1, this is a solution

# ==================================  save csv =====================================
#     This can works well and save lots of time.
#     pick you process_num based on cpu core(does not have to be 1 to 1, but not too many)
#     Over all, for a 19G data, if save by pandas directly need about 4000 seconds
#     with this method may need 700 seconds

import multiprocessing
from multiprocessing import Pool

import uuid
import subprocess
from subprocess import check_output
from functools import partial
import numpy as np

import pandas as pd
import os
import time


class CSVIO:
    def __init__(self, process_num=0):
        # Number of process which will going to create when input/output the file
        if process_num == 0:
            self.process_num = multiprocessing.cpu_count() - 1
        else:
            self.process_num = process_num
        # this id is used for create temprary folder
        self.unique_id = ""
        self.temp_folder = ""

    # create unique id
    def _create_unique_id(self):
        self.unique_id = uuid.uuid4()
        print(self.unique_id)

    def _remove_uniqule_id(self):
        self.unique_id = ""

    # create unique temp folder
    def _create_temp_folder(self, file_name):
        path, base_name = os.path.split(file_name)
        folder_name = "{}_{}".format(base_name, self.unique_id)
        self.temp_folder = os.path.join(path, folder_name)
        os.system("rm -rf {}".format(self.temp_folder))
        os.system("mkdir {}".format(self.temp_folder))

    def _remove_temp_folder(self):
        os.system("rm -rf {}".format(self.temp_folder))
        self.temp_folder = ""

    # Read the header
    def read_header(self, file):
        with open(file, 'r') as f:
            return [col.strip().lower() for col in f.readline().split(",")]

    def remove_header(self, file_name):
        cmd = "sed '1d' {} > tmp_file101010; mv tmp_file101010 {}".format(file_name, file_name)
        os.system(cmd)

    # create a list of file name based on the base_name and ending with a four_digit number
    def get_file_list(self, folder_name):
        file_list = []
        for name in os.listdir(folder_name):
            file_name = os.path.join(folder_name, name)
            if os.path.isfile(file_name):
                file_list.append(file_name)
        return file_list

    # create a list of file name based on the base_name and ending with a four_digit number
    def create_file_list(self, folder_name, base_name):
        return [os.path.join(folder_name, base_name + '{0:0=4d}'.format(n)) for n in range(self.process_num)]

        # ================================== load csv =====================================
#     The load csv function actually does not save much time
#     because the long time need for merge all dataset into one dataset
#     if no need for merge, this is a solution

    # generator function for count how many rows in the file
    def _make_gen(self, reader):
        b = reader(1024 * 1024)
        while b:
            yield b
            b = reader(1024 * 1024)

    # sum up number of '\n' as number of rows
    def count_row(self, file_name):
        f = open(file_name, 'rb')
        f_gen = self._make_gen(f.raw.read)
        return sum(buf.count(b'\n') for buf in f_gen)

    def line_count(self, filename):
        return int(check_output(["wc", "-l", filename]).split()[0])

    # split the csv file to sub-file by row
    def split_csv_by_line(self, file_name, base_name):
        strt = time.time()
        lines = self.line_count(file_name)
        if lines < 200000:
            line_no=200000
        else:
            line_no = int(lines / self.process_num) + 1
            # line_no = 100000
        # Delete folder if it exists
        cmd = "split -l {} {} -d -a 4 {}/{}".format(line_no, file_name, self.temp_folder, base_name)
        os.system(cmd)
        print("split done {} {}".format(cmd, time.time() -strt))

    def split_csv_by_size(self, file_name, base_name):
        strt = time.time()
        file_size = os.path.getsize(file_name)
        size = int(file_size / self.process_num)

        cmd = "split -C {} {} -d -a 4 {}/{}".format(size, file_name, self.temp_folder, base_name)
        print("split done  {}".format(time.time() -strt))


    # read a csv file
    def _read_csv(self, file_name):
        df = pd.read_csv(file_name, header=None, index_col=None)
        # print('load {}: done'.format(file_name))
        return df


    def load_csv(self, file_name):
        self._create_unique_id()
        self._create_temp_folder(file_name)

        file_cols = self.read_header(file_name)

        # split csv file to smaller file
        path, base_name = os.path.split(file_name)
        self.split_csv_by_line(file_name, base_name)

        # start pool of processes to load each of splited csv files
        file_list = self.get_file_list(self.temp_folder)
        self.remove_header(file_list[0])

        pool = Pool()
        func = partial(self._read_csv)
        results = pool.map(func, file_list)
        pool.close()
        # wait for all the csv file loaded
        pool.join()

        final_pd = pd.DataFrame(np.concatenate(results))
        final_pd.columns = file_cols

        self._remove_temp_folder()
        self._remove_uniqule_id()
        return final_pd

# ==================================  save csv =====================================
#     This can save lots of time. pick you process_num based on cpu core
    # (does not have to be 1 to 1, but not too many)

    # before save a dataset, split it to multi smaller dataset, and then save to file
    def split_pandas(self, df):
        return np.array_split(df, self.process_num)

    # save the small dataset to csv
    # the param is tuple of (filename, dataset)
    def _save_to_csv(self, param):
        param[1].to_csv(param[0], index=False, header=False)
        # print("save {}: done".format(param[0]))

    # save to csv
    def save_to_csv(self, df, sav_as_file):
        self._create_unique_id()
        self._create_temp_folder(sav_as_file)
        path, file_name = os.path.split(os.path.realpath(sav_as_file))

        # split dataset
        data_list = self.split_pandas(df)

        file_list = self.create_file_list(self.temp_folder, file_name)
        # zip the files and datasets
        param = zip(file_list, data_list)

        # start pool of processes to write dataset to disk
        pool = Pool()
        pool.map(self._save_to_csv, param)
        pool.close()
        # Wait for all dataset saved
        pool.join()

        # save the header to file_name_header
        header = df.columns.values
        header_string = ",".join(str(x) for x in header)

        header_file = os.path.join(self.temp_folder, "header")
        text_file = open(header_file, "w")
        text_file.write("{}\n".format(header_string))
        text_file.close()

        f_list_str = " ".join(str(x) for x in file_list)
        # merge all files
        os.system('cat {} {} > {}'.format(header_file, f_list_str, sav_as_file))
        # delete all temprary files
        # os.system('rm {} {}'.format(header_file, f_list_str))

        self._remove_temp_folder()
        self._remove_uniqule_id()




def test0():
    file_name = '/prod/user/sam/ent/crm/non_npi/fs/coaf/dev/sandbox/pwp558/csv_io/utilities/data/final_core.csv'

    # strt = time.time()
    # read_pd = pd.read_csv(file_name)
    # print ("======Total time read by Pandas {} seconds ===========".format(time.time() - strt))

    csv_io = CSVIO()

    strt = time.time()
    read_pd = csv_io.load_csv(file_name)
    print("==========Read data==== {} seconds ===========".format(time.time() - strt))

    path, filename = os.path.split(os.path.realpath("__file__"))
    strt = time.time()

    csv_io.save_to_csv(read_pd,  "/prod/user/sam/ent/crm/non_npi/fs/coaf/dev/sandbox/pwp558/csv_io/utilities/data/final_core_new.csv")
    # print("===========save data === {} seconds ===========".format(time.time() - strt))

    # strt = time.time()
    # csv_io = CSV_IO(64)
    # csv_io.save_to_csv(read_pd, path, "new_mth_64.csv" )
    # print ("=====save charge off data === {} seconds ===========".format(time.time() - strt))

    # strt = time.time()
    # file = os.path.join(path, "old_save.csv")
    # path_res = os.path.join(path, file)
    # read_pd.to_csv(path_res, index=False)
    print ("============== {} seconds ===========".format(time.time() - strt))

def test1():

    df1 = pd.read_csv('/home/user/loan_lvl_naco/loan_level_naco_1.csv')
    df2 = pd.read_csv('/home/user/loan_lvl_naco/loan_level_naco_2.csv')
    df3 = pd.read_csv('/home/user/loan_lvl_naco/loan_level_naco_3.csv')
    df4 = pd.read_csv('/home/user/loan_lvl_naco/loan_level_naco_4.csv')
    df5 = pd.read_csv('/home/user/loan_lvl_naco/loan_level_naco_5.csv')
    df6 = pd.read_csv('/home/user/loan_lvl_naco/loan_level_naco_6.csv')

    data = [df1, df2, df3, df4, df5, df6]

    final_pd = pd.DataFrame(np.concatenate(data))

    # strt = time.time()
    # read_pd = pd.read_csv(file_name)
    # print ("======Total time read by Pandas {} seconds ===========".format(time.time() - strt))

    csv_io = CSVIO()

    strt = time.time()

    csv_io.save_to_csv(final_pd,  "/home/user/loan_lvl_naco/loan_level_naco.csv")
    # print("===========save data === {} seconds ===========".format(time.time() - strt))

    # strt = time.time()
    # csv_io = CSV_IO(64)
    # csv_io.save_to_csv(read_pd, path, "new_mth_64.csv" )
    # print ("=====save charge off data === {} seconds ===========".format(time.time() - strt))

    # strt = time.time()
    # file = os.path.join(path, "old_save.csv")
    # path_res = os.path.join(path, file)
    # read_pd.to_csv(path_res, index=False)
    print ("============== {} seconds ===========".format(time.time() - strt))

def compare():

    df_base = pd.read_csv(
        '/prod/user/sam/ent/crm/non_npi/fs/coaf/dev/sandbox/pwp558/csv_io/utilities/data/final_core_new.csv')
    df = pd.read_csv('/prod/user/sam/ent/crm/non_npi/fs/coaf/dev/sandbox/pwp558/csv_io/utilities/data/final_core.csv')


    import pyproc

    report = pyproc.CompareReport(
        base_df=df,  # required
        compare_df=df_base,  # required
        id_column=['app_id', 'monthend'],  # required, accepts str or list
        sample_display=5,  # optional defaults to 5
        base_description='CCAR Base Python',  # optional defaults to None
        compare_description='CCAR Base SAS',  # optional defaults to None
        atol=0.0001,  # optional - absolute tolerance defaults to 0
        # rtol=1 # optional - relative tolerance defaults to 0
    )
    report.to_file(file_name='myreport.html')
if __name__ == "__main__":
    # test()
    # compare()
    test1()
