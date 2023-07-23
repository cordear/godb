package main

import (
	"bufio"
	"fmt"

	"godb/internal/tokenizer"
	"os"
)

func print_prompt() {
	fmt.Print("db > ")
}

func main() {
	reader := bufio.NewReader(os.Stdin)
	print_prompt()
	if text, err := reader.ReadString('\n'); err == nil {
		tk := tokenizer.NewTokenizer(text)
		for {
			tks, err := tk.PeekToken()
			if err == nil {
				fmt.Printf("%v\n", tks)
				tk.PopToken()
			} else {
				break
			}
		}
	} else {
		fmt.Println(err)
	}
}