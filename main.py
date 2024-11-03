import time

import data.load_dataset as data
import data.parallel_load_dataset as parallel_data

start_time = time.time()

parallel_data.main()

end_time = time.time()
elapsed_time = end_time - start_time
minutes, seconds = divmod(elapsed_time, 60)
print(f"Execution time: {int(minutes)} minutes and {seconds:.2f} seconds")