WORKDIR /app

COPY requirements.txt requirements.txt
RUN pip3 install -r requirements.txt

COPY . .

#CMD python3 main.py
CMD ["bash", "start.sh"]
