import re
from os import listdir
from os.path import isfile, join


def count_pdf_pages(file_path):
    rxcountpages = re.compile(r"/Type\s*/Page([^s]|$)", re.MULTILINE | re.DOTALL)
    with open(file_path, "rb") as temp_file:
        return len(rxcountpages.findall(temp_file.read()))


from numpy import allclose


def are_tables_united(table1_dict, table2_dict):
    if table2_dict['page'] == (table1_dict['page'] + 1):
        if len(table2_dict['cols']) == len(table1_dict['cols']):

            # extract the vertical coordinates of the tables
            _, y_bottom_table1, _, _ = table1_dict['_bbox']
            _, _, _, y_top_table2 = table2_dict['_bbox']

            page_height = 792

            # If the first table ends in the last 15% of the page
            # and the second table starts in the first 15% of the page
            if y_bottom_table1 < .15 * page_height and \
                    y_top_table2 > .85 * page_height:
                table1_cols = table1_dict['cols']
                table2_cols = table2_dict['cols']

                table1_cols_width = [col[1] - col[0] for col in table1_cols]
                table2_cols_width = [col[1] - col[0] for col in table2_cols]

                # evaluate if the column widths of the two tables are similar
                return (allclose(table1_cols_width, table2_cols_width, atol=3, rtol=0))

        else:
            return False
    else:
        return False


if __name__ == '__main__':
    mypath = r'C:\Users\mace\OneDrive - Zühlke Engineering AG\Documents\master-thesis-code\raw data\Potrošačka korpa'
    files = [f for f in listdir(mypath) if isfile(join(mypath, f))]
    # convert(mypath)
    # if not path.exists(join(mypath, 'pdfs')):
    #     mkdir(join(mypath, 'pdfs'))
    # for file in files:
    #     print(file)
    #     old_file_location = join(mypath, file)
    #     new_file_location=join(mypath, 'pdfs', file)
    #     # if '.doc' in file and not path.exists(join(mypath, 'pdfs', file)):
    #     #     new_file_name = file.split('.')[0] + '.pdf'
    #     #     print(f'New file name: {new_file_name}')
    #     #     new_file_location = join(mypath, 'pdfs', new_file_name)
    #     #     convert(old_file_location, new_file_location)
    #     # else:
    #     if '.pdf' in file and not path.exists(new_file_location):
    #         shutil.copy(old_file_location, join(mypath, 'pdfs', file))

    # files = [f for f in listdir(mypath) if isfile(join(mypath, 'pdfs', f))]
    # for file in files[:2]:
    #     tables = tabula.read_pdf(file, pages='all')
    #     print(tables[0])
    # tables[0].to_csv("foo.csv")

    # file='KUPOVNA MOC jun 2022.pdf'
    file = open('KUPOVNA MOC jun 2022.pdf', 'rb')
    import camelot.io as camelot

    tables = camelot.read_pdf('KUPOVNA MOC jun 2022.pdf', pages='all')

    # table_1 = tabula.read_pdf(file, pages=totalpages-2, stream=True,multiple_tables=True)
    # table_2 = tabula.read_pdf(file, pages=totalpages-1, stream=True,multiple_tables=True)
    # #
    # table1_dict = table_1.__dict__
    # table2_dict = table_2.__dict__
    # # #minimalna potrosacka korpa troclano domacinstvo
    # df_min_korpa_troclana_korpa = pd.concat([pd.DataFrame(*table_1), pd.DataFrame(*table_2)], axis=0)
    # print(df_min_korpa_troclana_korpa)

    # Odnos prihoda i potrosacke korpe
    # Kupovna moc po gradovima
    # to_remove=[]
    # print(tables[1].columns)
    # print(tables[2])
    # df_odnos_prihoda_i_korpe = pd.DataFrame([])
    # for tbl in tables:
    #     print(tbl.shape)
    #     if 'Београд' in str(tbl.iloc[:,0]):
    #         break
    #     df_odnos_prihoda_i_korpe= pd.concat([df_odnos_prihoda_i_korpe, tbl], axis=0)
    #     to_remove.append(id(tbl))
    # #tables=[t for t in tables if id(t) not in to_remove]
    # print("Total tables extracted:", len(tables))
    #
    # df_odnos_prihoda_i_korpe.drop(index=df_odnos_prihoda_i_korpe.index[:7], axis=0, inplace=True)
    # df_odnos_prihoda_i_korpe.columns=['Godina-mesec','Prosecna mesecna neto zarada po zaposlenom','Korpe','Odnos prosecne korpe i zarade','Odnos minimalne korpe i zarade']
    # df_odnos_prihoda_i_korpe[['Prosecna potrosacka korpa','Nova potrosacka korpa']]=df_odnos_prihoda_i_korpe.Korpe.str.split(expand=True)
    #
    # print(df_odnos_prihoda_i_korpe)# tables[0].to_csv("foo.csv")
    # print("Total tables extracted:", len(tables))

    # pages=count_pdf_pages(file_path='KUPOVNA MOC jun 2022.pdf')
    # print(f'Number of pages is {pages}')
    # for pageiter in range(8):
    #     df = tabula.read_pdf('KUPOVNA MOC jun 2022.pdf', pages=pageiter + 1, guess=False)
    #     # If you want to change the table by editing the columns you can do that here.
    #     df_combine = pd.concat([df, df_combine], )  # again you can choose between merge or concat as per your need
    #
    # print(df_combine)

    #
    #
