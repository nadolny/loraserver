package storage

import (
	"testing"
	"time"

	"github.com/brocaar/loraserver/internal/common"
	"github.com/brocaar/loraserver/internal/test"
	. "github.com/smartystreets/goconvey/convey"
)

func TestGatewayConfiguration(t *testing.T) {
	conf := test.GetConfig()
	db, err := common.OpenDatabase(conf.PostgresDSN)
	if err != nil {
		t.Fatal(err)
	}

	Convey("Given a clean database", t, func() {
		test.MustResetDB(db)

		Convey("When creating gateway configuration", func() {
			gc := GatewayConfiguration{
				Channels: []int64{0, 1, 2},
				ExtraChannels: []ExtraChannel{
					{
						Modulation:       ModulationLoRa,
						Frequency:        868700000,
						Bandwidth:        125,
						SpreadingFactors: []int64{10, 11, 12},
					},
					{
						Modulation: ModulationLoRa,
						Frequency:  868900000,
						Bandwidth:  125,
						Bitrate:    50000,
					},
				},
			}
			So(CreateGatewayConfiguration(db, &gc), ShouldBeNil)
			gc.CreatedAt = gc.CreatedAt.UTC().Truncate(time.Millisecond)
			gc.UpdatedAt = gc.UpdatedAt.UTC().Truncate(time.Millisecond)

			Convey("Then it can be retrieved", func() {
				gc2, err := GetGatewayConfiguration(db, gc.ID)
				So(err, ShouldBeNil)

				gc2.CreatedAt = gc2.CreatedAt.UTC().Truncate(time.Millisecond)
				gc2.UpdatedAt = gc2.UpdatedAt.UTC().Truncate(time.Millisecond)
				So(gc2, ShouldResemble, gc)
			})

			Convey("Then it can be deleted", func() {
				So(DeleteGatewayConfiguration(db, gc.ID), ShouldBeNil)
				_, err := GetGatewayConfiguration(db, gc.ID)
				So(err, ShouldEqual, ErrDoesNotExist)
			})

			Convey("Then it can be updated", func() {
				gc.Channels = []int64{0, 1}
				gc.ExtraChannels = []ExtraChannel{
					{
						Modulation: ModulationLoRa,
						Frequency:  868900000,
						Bandwidth:  125,
						Bitrate:    50000,
					},
					{
						Modulation:       ModulationLoRa,
						Frequency:        868700000,
						Bandwidth:        125,
						SpreadingFactors: []int64{10, 11, 12},
					},
				}
				So(UpdateGatewayConfiguration(db, &gc), ShouldBeNil)
				gc.UpdatedAt = gc.UpdatedAt.UTC().Truncate(time.Millisecond)

				gc2, err := GetGatewayConfiguration(db, gc.ID)
				So(err, ShouldBeNil)
				gc2.CreatedAt = gc2.CreatedAt.UTC().Truncate(time.Millisecond)
				gc2.UpdatedAt = gc2.UpdatedAt.UTC().Truncate(time.Millisecond)
				So(gc2, ShouldResemble, gc)
			})
		})
	})
}
