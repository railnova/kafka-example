# Railnova Kafka example
This repository provides an example to connect to our Kafka. It serves to verify that a kafka environment has been correctly configured.

This repository contains 3 files:

- In the file `requirements.txt` you will find the library that need to be installed.
- In the file `settings.py`, you will need to set the variables we sent you.
- In the file `main.py`, you will find the logic.


## Installation

1. Install the dependencies using `pip` and using python3.9 (python3.10 and the kafka library are not fully compatible yet on 02-06-2022)
    `pip install -r requirements.txt`
2. Copy the `ca.pem` file we sent you next in this direcotry next to this `README.md` file;
3. In the file `settings.py`, set the variable to the value we sent you, and choose a valid `KAFKA_GROUP_ID` value.

## Getting your first message

To get your first message, run the main file by executing `python main.py`.

The following output is expected:

- `Connection to Railnova Kafka successful`
    - The client was able to connect to the hosted kafka instance
- `Polling Kafka with a timeout of 120s`
    - The client is waiting for a new message
    - It is not unlikely that you might time out here, depending on how many assets are sending messages to the topic.
    If the issue is persistent, please get in contact.
- `Key: [...]`, `Value : [...]`
    - The content of the received message
- `One message succesfully consumed`
    - The final message before the script returns
    - **This message means that the Kafka environment works as expected**
