import time
import subprocess

# PROJECT SCHEDULER
def run_project():
    """
    Main function to perform all steps of the project. No arguments required.
    """
    run_movies_spider(final_wait=0)

# FUNCTIONS DEDICATED TO PROJECT STAGES (directly called by the scheduler)
def run_movies_spider(final_wait: int = 0):
    """
    Run the movies spider to scrape movies and all the related data.

    Parameters:
        final_wait (int): Number of seconds to wait after running the spider.
                          Default value is `0`, meaning no wait time.
    """
    print("Running `movies_spider`...".upper())
    subprocess.run(['scrapy', 'crawl', 'movies_spider'])
    print("\n`movies_spider` successfully completed".upper())
    wait(duration=final_wait)

# HELPER FUNCTIONS (auxiliary functions. Not directly called by the scheduler)
def wait(duration: int = 0):
    """
    Pause the program for a specified duration.

    Parameters:
        duration (int): Duration of the break in seconds.

    Returns:
        None
    """
    if isinstance(duration, int) and duration > 0:
        print(f"Waiting for {duration} seconds...")
        time.sleep(duration)

# MAIN BLOCK (end of file)
if __name__ == "__main__":
    run_project()