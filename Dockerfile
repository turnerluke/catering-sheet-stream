# Lambda base image for Docker from AWS
FROM public.ecr.aws/lambda/python:latest
LABEL authors="turner"


# Copy all code and lambda handler
COPY lambda_handler.py .
COPY requirements.txt .
COPY _submodules _submodules

# Install packages
RUN python3 -m pip install -r requirements.txt
RUN python3 -m pip install _submodules/ --upgrade
RUN yum install -y gcc-c++ pkgconfig poppler-cpp-devel



# Run lambda handler
CMD ["lambda_handler.handler"]