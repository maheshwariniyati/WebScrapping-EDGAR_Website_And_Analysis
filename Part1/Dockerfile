FROM python:3-onbuild


RUN pip3 install requests
RUN pip3 install BeautifulSoup4
RUN pip3 install boto
RUN pip3 install boto3
RUN pip3 install Key
RUN pip3 install bs4

COPY app.py /src/app.py

CMD ["python", "./app.py"]