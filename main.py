import argparse

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--d1', type=str, required=True, help='path to the first dataset')
    parser.add_argument('--d2', type=str, required=True, help='path to the second dataset')
    parser.add_argument('--f','--list', nargs='+', help='Filter on Country', required=True)

    opt = parser.parse_args()