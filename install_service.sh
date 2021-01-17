SERVICE_NAME=watering_manager.service
echo "Stop service"
sudo systemctl stop $SERVICE_NAME
if [ ! -f venv/bin/activate ]; then
  echo "Install virtual python environment"
  python3.8 -m venv venv
fi
. venv/bin/activate
echo "Install python requirements"
pip install -r ./requirements.txt
echo "Copy service"
sudo cp $SERVICE_NAME /etc/systemd/system/
echo "Adapt service for current working dictionary"
sudo sed -i "s@PWD@$PWD@g" /etc/systemd/system/$SERVICE_NAME
echo "Enable service"
sudo systemctl enable $SERVICE_NAME
echo "Start service"
sudo systemctl start $SERVICE_NAME
echo "Status of service"
sudo systemctl status $SERVICE_NAME