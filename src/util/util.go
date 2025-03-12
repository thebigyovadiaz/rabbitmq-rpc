package util

import "log"

func LogFailOnError(err error, msg string) {
	if err != nil {
		log.Panicf("%s: %s", msg, err)
	}
}

func LogSuccessful(msg string) {
	log.Println(msg)
}
