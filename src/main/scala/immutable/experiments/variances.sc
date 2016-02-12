class GParent

class Parent extends GParent

class Child extends Parent

class Box[+A]

class Box2[-A]

def foo(x : Box[Parent]) : Box[Parent] = identity(x)

def bar(x : Box2[Parent]) : Box2[Parent] = identity(x)

foo(new Box[Child]) // success

//foo(new Box[GParent]) // type error
//
//bar(new Box2[Child]) // type error

bar(new Box2[GParent]) // success