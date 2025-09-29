package keys

import "fmt"

// KeyGenerator generates Redis keys for a queue
type KeyGenerator struct {
	queueName string
}

// NewKeyGenerator creates a new key generator for the given queue name
func NewKeyGenerator(queueName string) *KeyGenerator {
	return &KeyGenerator{
		queueName: queueName,
	}
}

// Job returns the key for a specific job
func (kg *KeyGenerator) Job(jobID string) string {
	return fmt.Sprintf("ox:%s:job:%s", kg.queueName, jobID)
}

// Waiting returns the key for the waiting list
func (kg *KeyGenerator) Waiting() string {
	return fmt.Sprintf("ox:%s:waiting", kg.queueName)
}

// Active returns the key for the active list
func (kg *KeyGenerator) Active() string {
	return fmt.Sprintf("ox:%s:active", kg.queueName)
}

// Delayed returns the key for the delayed set
func (kg *KeyGenerator) Delayed() string {
	return fmt.Sprintf("ox:%s:delayed", kg.queueName)
}

// State returns the key for a specific state set
func (kg *KeyGenerator) State(state string) string {
	return fmt.Sprintf("ox:%s:%s", kg.queueName, state)
}

// Paused returns the key for the paused flag
func (kg *KeyGenerator) Paused() string {
	return fmt.Sprintf("ox:%s:paused", kg.queueName)
}

// Meta returns the key for queue metadata
func (kg *KeyGenerator) Meta() string {
	return fmt.Sprintf("ox:%s:meta", kg.queueName)
}
