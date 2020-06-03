package main

import "time"

type DbUpdateRecord struct {
	ID        uint `gorm:"primary_key"`
	CreatedAt time.Time

	UpdateFile string `gorm:"type:varchar(32);UNIQUE_INDEX"`
}
