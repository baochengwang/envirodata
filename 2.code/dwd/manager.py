# ... read config ...

from output import write_to_postgres

from input import get_from_dwd

for input_config in config["input"]:
    times, dataslices = get_from_dwd(input_config)
    for t, d in zip(times, dataslices):
        write_to_postgres(t, d, output_config)
