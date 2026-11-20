In this stage, you will add support for creating a new list using the RPUSH command.
The RPUSH Command

The RPUSH command is used to append elements to a list. If the list does not exist, it is created first.

Example usage:

# Creating a new list with a single element
> RPUSH list_key "foo"
(integer) 1

# Appending a single element to an existing list
> RPUSH list_key "bar"
(integer) 2

The return value is the number of elements in the list after appending. This value is encoded as a RESP integer.
Tests

The tester will execute your program like this:

./your_program.sh

It will then send the following command to your program:

$ redis-cli RPUSH list_key "element"

The tester will verify that the response to the command is :1\r\n, which is 1 (the number of elements in the list), encoded as a RESP integer.
Notes

    In this stage, you will only need to handle creating a new list with a single element. We will get to handling RPUSH for existing lists and multiple elements in later stages.

