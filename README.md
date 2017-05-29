## Home Task

This service is using default rabbitmq (port/user/password) configuration.
To install rabbitmq locally:
```
sudo apt-get install rabbitmq-server
```

##To start server
```
python service.py server
```

##To send message
```
python servoce.py client -i test.json
```