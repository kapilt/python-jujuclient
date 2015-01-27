.venv:
	sudo apt-get install -y python-dev python-virtualenv python-yaml
	virtualenv .venv --system-site-packages
	.venv/bin/pip install -U pip
	.venv/bin/pip install -U distribute

.venv3:
	sudo apt-get install -y python3-dev python-virtualenv python3-yaml
	virtualenv .venv3 --python=python3 --system-site-packages
	.venv3/bin/pip install -U pip
	.venv3/bin/pip install -U distribute
