struct User {
	1:required i32 age;
	2:required string name;
}

service EchoSerice 
{
	string echo(1:User user)
}