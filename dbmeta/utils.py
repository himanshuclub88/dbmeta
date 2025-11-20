import os
import json
import shutil
import random
from datetime import datetime, timedelta

def generate_sampledate(base_path = "genrated_batch_logs", number_of_days_to_genrate=10,delete_if_exist=False):
    """
    Generates sample date logs over a specified number of days.

    Args:
        base_path (str): The base directory for the generated logs.
        number_of_days_to_genrate (int): The number of days of logs to generate.
        delete_if_exist (bool): If True, existing data in the base_path will be deleted 
                                before generating new data. Use with caution.

    Caution:
        Setting 'delete_if_exist' to True will permanently remove all older data 
        in the specified 'base_path'.
    """
    # Clean previous logs
    
    base=os.path.join(os.getcwd(),base_path)


    if os.path.exists(base):
        print('PATH ALREADY EXIST........=', base)

        if delete_if_exist:
            # Attempt to delete the directory with a try-except block
            try:
                shutil.rmtree(base)
                os.makedirs(base, exist_ok=True)
                print(f"Existing path deleted and recreated: {base}")

            except PermissionError:
                # Handle the specific case where the script lacks permissions
                print(f"----------------------------------------------------------------------")
                print(f"PERMISSION ERROR: The script cannot delete the path: {base}")
                print(f"Please ensure all files in this directory are closed.")
                print(f"You may need to MANUALLY delete the folder or run the script as an administrator/superuser.")
                print(f"----------------------------------------------------------------------")
                
                # Optionally, you might still want to halt execution here since you can't proceed as intended
                raise PermissionError(f"Insufficient permissions to delete path: {base}. Manual deletion required.")

            except OSError as e:
                # Handle other potential OS errors (e.g., directory not empty but files are in use)
                print(f"----------------------------------------------------------------------")
                print(f"OS ERROR during deletion of {base}: {e}")
                print(f"Manual intervention may be required.")
                print(f"----------------------------------------------------------------------")
                raise

        else:
            # This block runs if the path exists AND delete_if_exist is False
            print(f"SELECT delete_if_exist=True to delete the path or create sampledeta at another path currentpath:{base}")
            # Raise an informative error because we cannot proceed safely without deleting the existing path
            raise FileExistsError(f"The path already exists: {base}. Set 'delete_if_exist=True' to overwrite, or specify a different base path.")



    # -----------------------------
    # Function to generate sample metadata
    # -----------------------------
    def gen_metadata(run_date, status):
        start_time = run_date.strftime("%Y-%m-%dT%H:%M:%S")

        duration = random.randint(120, 1200)
        rows_in = random.randint(100_000, 900_000)
        rows_out = rows_in - random.randint(50, 2000)

        files_read = random.randint(5, 25)

        return {
            "execution_info": {
                "start_time": start_time,
                "status": status,
                "duration_sec": duration
            },
            "stats": {
                "rows_in": rows_in,
                "rows_out": rows_out
            },
            "inputs": {
                "files_read": files_read,
                "source": "landing/finance/"
            }
        }

    # -----------------------------
    # Generate 10 days of JobA metadata
    # -----------------------------
    number_of_days_to_genrate=10
    job_name = "JobA"
    today = datetime.now()
    start_date = today - timedelta(days=number_of_days_to_genrate)

    for i in range(number_of_days_to_genrate):
        run_date = start_date + timedelta(days=i)
        timestamp_str = run_date.strftime("%Y-%m-%d_%H%M%S")

        folder_name = f"{job_name}_{timestamp_str}"
        folder_path = os.path.join(base, folder_name)
        os.makedirs(folder_path, exist_ok=True)

        # Randomly pick SUCCESS or FAILED
        status = random.choice(["SUCCESS", "FAILED"])

        # Generate metadata.json
        metadata = gen_metadata(run_date, status)
        with open(os.path.join(folder_path, "metadata.json"), "w") as f:
            json.dump(metadata, f, indent=4)

        # Create sample log files
        with open(os.path.join(folder_path, "run.log"), "w") as f:
            f.write(f"[{timestamp_str}] Job {job_name} started\n")
            f.write(f"[{timestamp_str}] Status: {status}\n")
            f.write("Processing completed.\n")

        with open(os.path.join(folder_path, "error.txt"), "w") as f:
            if status == "FAILED":
                f.write("Error: Sample failure occurred in transformation stage.\n")
            else:
                f.write("No errors.\n")

        # Create debug and temp directories
        os.makedirs(os.path.join(folder_path, "debug"), exist_ok=True)
        os.makedirs(os.path.join(folder_path, "temp"), exist_ok=True)

    print(f"Generated {number_of_days_to_genrate} days of sample job run folders with metadata and logs. @",base_path)
    print(
        f"""creat a intance of db
        >>>db = FolderDB(base_path={base_path}, base_metadata="metadata.json"))""")