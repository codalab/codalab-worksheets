prev=0x4549793056ec472e9a6e4c178facfbe8; for i in {1..100}; do prev=$(cl run :$prev 'echo hello'); done
