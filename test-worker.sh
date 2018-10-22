# Test working for docker < max & dependency < max
./venv/bin/python worker/codalabworker/main.py \
 --server https://worksheets.codalab.org \
 --max-image-cache-size 10k
 --max-work-dir-size 10k
 --verbose

 # Test cleanup for docker