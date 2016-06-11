use constant BYTES_IN_MB => 1024 * 1024;

my $time = $ARGV[0];
my $memory = $ARGV[1];
my $disk = $ARGV[2];

my $mem_buf = "";
my $bytes_written = 0;

for($i = 0; $i < $time; $i++) {
    printf "time = %d, mem = %d MB, disk = %d MB\n",
        $i, length($mem_buf) / BYTES_IN_MB, $bytes_written / BYTES_IN_MB;

    # Allocate memory
    my $new_mem_bytes = int(BYTES_IN_MB * $memory / $time);
    $mem_buf .= "\0" x $new_mem_bytes;

    # Write to disk
    my $new_disk_bytes = int(BYTES_IN_MB * $disk / $time);
    $bytes_written += $new_disk_bytes;
    system(sprintf('dd if=/dev/zero of=output bs=%s count=1 oflag=append conv=notrunc', $new_disk_bytes));

    sleep 1;
}
