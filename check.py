import argparse
import os
import time

def main(dir):
    def get_num_files(dir):
        is_dir = os.path.isdir(dir)
        if is_dir:
            print(f"Directory exists: {dir}")
        else:
            raise RuntimeError(f"Directory went missing: {dir}")

        files = os.listdir(dir)
        return len(files)

    count = get_num_files(dir)
    print(f"Original count={count}")

    while True:
        new_count = get_num_files(dir)
        if count != new_count:
            raise RuntimeError(f"Missing files - expected={count}, got={new_count}")
        time.sleep(5)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='checks files'
    )
    parser.add_argument(
        'directory',
        type=str,
        help='directory to check',
    )

    # Parse args and run this script
    args = parser.parse_args()
    main(args.directory)
