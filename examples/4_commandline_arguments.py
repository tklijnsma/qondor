import argparse
parser = argparse.ArgumentParser()
parser.add_argument("some_string", type=str)
args = parser.parse_args()
print(args.some_string)