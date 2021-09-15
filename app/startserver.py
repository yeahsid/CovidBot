import subprocess
from colorama import Fore, Back, Style
try:  # Runs gunicorn app
    execution = 'gunicorn -w 3 -k uvicorn.workers.UvicornWorker app:app -b 0.0.0.0:8000 '
    print(
        Fore.GREEN + "App is running successfully. Go to https://graphql.itsezsid.com or "
                     "0.0.0.0:8000/graphql to access the GraphQL endpoint" + Style.RESET_ALL)
    subprocess.getstatusoutput(execution)

except KeyboardInterrupt:
    try:  # Stops gunicorn using pkill [Kills Process]
        execution = 'pkill gunicorn'
        subprocess.getstatusoutput(execution)
        print(Fore.GREEN + "Gunicorn Stopped Successfully" + Style.RESET_ALL)

    except:  # Except block
        print(
            Fore.RED + "Unable To stop Gunicorn Directly.\n" + Fore.GREEN + "Please enter the backend directory and run 'pkill gunicorn'" +
            + Style.RESET_ALL)
except:  # Except block if cli cant run gunicorn
    print(Fore.RED + "Unable To Run Gunicorn Directly.\n" + Fore.GREEN + "Please enter the backend directory "
                                                                         "and run 'gunicorn -w 3 -k "
                                                                         "uvicorn.workers.UvicornWorker "
                                                                         "app:app -b "
                                                                         "0.0.0.0:8000'" +
          + Style.RESET_ALL)
