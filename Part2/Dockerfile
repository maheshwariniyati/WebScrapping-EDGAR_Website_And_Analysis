FROM python:3-onbuild


RUN pip3 install requests
RUN pip3 install BeautifulSoup4
RUN pip3 install boto
RUN pip3 install boto3
RUN pip3 install Key
RUN pip3 install bs4
RUN pip3 install pandas
RUN pip3 install numpy
RUN pip3 install matplotlib

COPY Problem2.py /src/Problem2.py

CMD ["python", "./Problem2.py"]