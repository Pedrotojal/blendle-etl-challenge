# blendle-etl-challenge
Getting hired by Blendle

1. [The Challenge Explained](The-Challenge.md)

2. Details 

The original events.json was not prepared to be read, I had to tweak it. Did it with this:
```
sed '1s/^/[/;$!s/$/,/;$s/$/]/'
```
