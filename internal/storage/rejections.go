package storage

import (
	"context"

	"payway/internal/broker"
)

func (s *Store) SaveRejection(ctx context.Context, message broker.RejectedMessage) error {
	_, err := s.DB.Exec(ctx, `INSERT INTO rejected_messages
		(consumer_group, topic, partition, message_offset, message_key, payload, reason)
		VALUES ($1, $2, $3, $4, $5, $6, $7)
		ON CONFLICT (consumer_group, topic, partition, message_offset) DO NOTHING`,
		message.Group, message.Topic, message.Partition, message.Offset,
		message.Key, message.Payload, message.Reason)
	return err
}
