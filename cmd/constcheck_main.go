package main

import (
	"fmt"
	runnerapi "github.com/rouroumaibing/software-distribution-platform-runner/api/v1alpha1"
)

func main() { fmt.Println("EXEC:", runnerapi.AgentOpTypeExec, "UPG:", runnerapi.AgentOpTypeUpgrade) }
