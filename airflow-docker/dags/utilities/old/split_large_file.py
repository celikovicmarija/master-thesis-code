import pandas as pd

INFILE = 'raw_data\\geoapify\\place_details_more_details.csv'  # Path to input file
OUTPATH = 'raw_data\\geoapify\\partial'  # target directory


def main():
    chunk_size = 500000  # lines

    def write_chunk(part, lines):
        with open('chunk' + str(part) + '.csv', 'w') as f_out:
            f_out.write(header)
            f_out.writelines(lines)

    with open(INFILE, 'r') as f:
        count = 0
        header = f.readline()
        lines = []
        for line in f:
            count += 1
            lines.append(line)
            if count % chunk_size == 0:
                write_chunk(count // chunk_size, lines)
                lines = []
        # write remainder
        if len(lines) > 0:
            write_chunk((count // chunk_size) + 1, lines)


if __name__ == '__main__':
    first = True
    for i, chunk in enumerate(pd.read_csv(INFILE, chunksize=500000)):
        if first:
            chunk.to_csv('chunk{}.csv'.format(i), index=False)
            first = False
