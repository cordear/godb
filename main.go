package main

import (
	"bufio"
	"fmt"

	"godb/internal/parser"
	"os"
)

func print_prompt() {
	fmt.Print("db > ")
}

func main() {
	reader := bufio.NewReader(os.Stdin)
	print_prompt()
	if text, err := reader.ReadString('\n'); err == nil {
		p, err := parser.Parse(text)
		if err != nil {
			panic(err)
		}
		switch ct := p.(type) {
		case parser.CreateTableStatement:
			fmt.Println(ct)
		case parser.InsertStatement:
			fmt.Println(ct)
		case parser.SelectStatement:
			fmt.Println(ct)
		}
	} else {
		fmt.Println(err)
	}
}
