package main

type genericFn[T any] func() T

func something[T any](a T) {

}
