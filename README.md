# ParameterServer
ParameterServer is an implementation for [dmlc/ps-lite](https://github.com/dmlc/ps-lite) in java, which is a light and efficient
implementation of the parameter server framework, and can be easily used to implementing distributed deep learning training framework, 
such as [TensorFlow](https://github.com/tensorflow/tensorflow), [PyTorch](https://github.com/pytorch/pytorch), 
[MXNet](https://github.com/apache/incubator-mxnet), etc.

## Build

### How to build
ParameterServer is built using [Gradle](https://github.com/gradle/gradle). to build ParameterServer, run:

    ./gradlew build

This will run tests automatically, if you want build without running tests, run:

    ./gradlew build -x test

The jar required to run ParameterServer will be located in `./build/libs/`. run:

    java -cp "build/libs/*" cn.daugraph.ps.Bootstrap

## Architecture
// TODO
