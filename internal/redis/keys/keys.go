package keys

import "fmt"

type KeyGenerator struct {
	queueName string
}

// Creates a new key generator for the given queue name
func NewKeyGenerator(queueName string) *KeyGenerator {
	return &KeyGenerator{
		queueName: queueName,
	}
}

// Returns the key for a specific job
func (kg *KeyGenerator) Job(jobID string) string {
	return fmt.Sprintf("ox:%s:job:%s", kg.queueName, jobID)
}

// Returns the key for the waiting list (actual priority queue)
func (kg *KeyGenerator) Waiting() string {
	return fmt.Sprintf("ox:%s:waiting", kg.queueName)
}

func (kg *KeyGenerator) Active() string {
	return fmt.Sprintf("ox:%s:active", kg.queueName)
}

func (kg *KeyGenerator) Delayed() string {
	return fmt.Sprintf("ox:%s:delayed", kg.queueName)
}

func (kg *KeyGenerator) Completed() string {
	return fmt.Sprintf("ox:%s:completed", kg.queueName)
}

func (kg *KeyGenerator) Failed() string {
	return fmt.Sprintf("ox:%s:failed", kg.queueName)
}

func (kg *KeyGenerator) State(state string) string {
	return fmt.Sprintf("ox:%s:%s", kg.queueName, state)
}

// Returns the key for the paused flag
func (kg *KeyGenerator) Paused() string {
	return fmt.Sprintf("ox:%s:paused", kg.queueName)
}

// Returns the key for queue metadata
func (kg *KeyGenerator) Meta() string {
	return fmt.Sprintf("ox:%s:meta", kg.queueName)
}
