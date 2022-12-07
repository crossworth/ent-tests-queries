package schema

import (
	"entgo.io/ent"
	"entgo.io/ent/schema/field"
)

// Node holds the schema definition for the Node entity.
type Node struct {
	ent.Schema
}

// Fields of the Node.
func (Node) Fields() []ent.Field {
	return []ent.Field{
		field.String("from"),
		field.String("to"),
	}
}

// Edges of the Node.
func (Node) Edges() []ent.Edge {
	return nil
}
