package storage

import (
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/lib/pq"
	"github.com/satori/go.uuid"
	log "github.com/sirupsen/logrus"
)

// Modulations
const (
	ModulationFSK  = "FSK"
	ModulationLoRa = "LORA"
)

// ExtraChannel defines an extra channel for the gateway configuration.
type ExtraChannel struct {
	Modulation       string  `db:"modulation"`
	Frequency        int     `db:"frequency"`
	Bandwidth        int     `db:"bandwidth"`
	Bitrate          int     `db:"bitrate"`
	SpreadingFactors []int64 `db:"spreading_factors"`
}

// GatewayConfiguration defines a gateway configuration.
type GatewayConfiguration struct {
	ID            string         `db:"id"`
	CreatedAt     time.Time      `db:"created_at"`
	UpdatedAt     time.Time      `db:"updated_at"`
	Channels      []int64        `db:"channels"`
	ExtraChannels []ExtraChannel `db:"-"`
}

// CreateGatewayConfiguration creates the given gateway configuration.
// As this will execute multiple SQL statements, it is recommended to perform
// this within a transaction.
func CreateGatewayConfiguration(db sqlx.Execer, c *GatewayConfiguration) error {
	now := time.Now()
	c.CreatedAt = now
	c.UpdatedAt = now

	if c.ID == "" {
		c.ID = uuid.NewV4().String()
	}

	_, err := db.Exec(`
		insert into gateway_configuration (
			id,
			created_at,
			updated_at,
			channels
		) values ($1, $2, $3, $4)`,
		c.ID,
		c.CreatedAt,
		c.UpdatedAt,
		pq.Array(c.Channels),
	)
	if err != nil {
		return handlePSQLError(err, "insert error")
	}

	for _, ec := range c.ExtraChannels {
		_, err := db.Exec(`
			insert into gateway_configuration_extra_channel (
				gateway_configuration_id,
				modulation,
				frequency,
				bandwidth,
				bitrate,
				spreading_factors
			) values ($1, $2, $3, $4, $5, $6)`,
			c.ID,
			ec.Modulation,
			ec.Frequency,
			ec.Bandwidth,
			ec.Bitrate,
			pq.Array(ec.SpreadingFactors),
		)
		if err != nil {
			return handlePSQLError(err, "insert error")
		}
	}

	log.WithFields(log.Fields{
		"id": c.ID,
	}).Info("gateway configuration created")

	return nil
}

// GetGatewayConfiguration returns the gateway configuration matching the
// given ID.
func GetGatewayConfiguration(db sqlx.Queryer, id string) (GatewayConfiguration, error) {
	var c GatewayConfiguration
	err := db.QueryRowx(`
		select
			id,
			created_at,
			updated_at,
			channels
		from gateway_configuration
		where
			id = $1`,
		id,
	).Scan(
		&c.ID,
		&c.CreatedAt,
		&c.UpdatedAt,
		pq.Array(&c.Channels),
	)
	if err != nil {
		return c, handlePSQLError(err, "select error")
	}

	rows, err := db.Query(`
		select
			modulation,
			frequency,
			bandwidth,
			bitrate,
			spreading_factors
		from gateway_configuration_extra_channel
		where
			gateway_configuration_id = $1
		order by id`,
		id,
	)
	if err != nil {
		return c, handlePSQLError(err, "select error")
	}
	defer rows.Close()

	for rows.Next() {
		var ec ExtraChannel
		err := rows.Scan(
			&ec.Modulation,
			&ec.Frequency,
			&ec.Bandwidth,
			&ec.Bitrate,
			pq.Array(&ec.SpreadingFactors),
		)
		if err != nil {
			return c, handlePSQLError(err, "select error")
		}
		c.ExtraChannels = append(c.ExtraChannels, ec)
	}

	return c, nil
}

// UpdateGatewayConfiguration updates the given gateway configuration.
// As this will execute multiple SQL statements, it is recommended to perform
// this within a transaction.
func UpdateGatewayConfiguration(db sqlx.Execer, c *GatewayConfiguration) error {
	c.UpdatedAt = time.Now()
	res, err := db.Exec(`
		update gateway_configuration
		set
			updated_at = $2,
			channels = $3
		where
			id = $1`,
		c.ID,
		c.UpdatedAt,
		pq.Array(c.Channels),
	)
	if err != nil {
		return handlePSQLError(err, "update error")
	}

	ra, err := res.RowsAffected()
	if err != nil {
		return handlePSQLError(err, "get rows affected error")
	}
	if ra == 0 {
		return ErrDoesNotExist
	}

	// This could be optimized by creating a diff of the actual extra channels
	// and the wanted. As it is not likely that this data changes really often
	// the 'simple' solution of re-creating all the extra channels has been
	// implemented.
	_, err = db.Exec(`
		delete from gateway_configuration_extra_channel
		where
			gateway_configuration_id = $1`,
		c.ID,
	)
	if err != nil {
		return handlePSQLError(err, "delete error")
	}
	for _, ec := range c.ExtraChannels {
		_, err := db.Exec(`
			insert into gateway_configuration_extra_channel (
				gateway_configuration_id,
				modulation,
				frequency,
				bandwidth,
				bitrate,
				spreading_factors
			) values ($1, $2, $3, $4, $5, $6)`,
			c.ID,
			ec.Modulation,
			ec.Frequency,
			ec.Bandwidth,
			ec.Bitrate,
			pq.Array(ec.SpreadingFactors),
		)
		if err != nil {
			return handlePSQLError(err, "insert error")
		}
	}

	log.WithFields(log.Fields{
		"id": c.ID,
	}).Info("gateway configuration updated")

	return nil
}

// DeleteGatewayConfiguration deletes the gateway-configuration matching the
// given ID.
func DeleteGatewayConfiguration(db sqlx.Execer, id string) error {
	res, err := db.Exec(`
		delete from gateway_configuration
		where
			id = $1`,
		id,
	)
	if err != nil {
		return handlePSQLError(err, "delete error")
	}

	ra, err := res.RowsAffected()
	if err != nil {
		return handlePSQLError(err, "get rows affected error")
	}
	if ra == 0 {
		return ErrDoesNotExist
	}

	log.WithFields(log.Fields{
		"id": id,
	}).Info("gateway configuration deleted")

	return nil
}
