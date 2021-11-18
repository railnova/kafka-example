# Railnova Kafka example
This repository provides an example to connect to our Kafka.

This repository contains 3 files:

- In the file `requirements.txt` you will find the library that need to be installed.

- In the file `settings.py`, you will need to set the variables we sent you.

- In the file `main.py`, you will find the logic.


## Installation

1. Install the dependencies using `pip`

`pip install -r requirements.txt`

2. Copy the `ca.pem` file we sent you next in this direcotry next to this `README.md` file;

3. In the file `settings.py`, set the variable to the value we sent you

Done.

## Getting your first message

To get your first message, run the main file

`python main.py`

You should see `"Connection to Railnova Kafka successful !"`, it means that the authentification worked.

You should then see your first message printed, followed by `"All good!"`, meaning that you got your first message!
