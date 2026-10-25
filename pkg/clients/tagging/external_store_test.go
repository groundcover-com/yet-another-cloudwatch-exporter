package tagging_test

import (
	"context"
	"errors"
	"log/slog"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/prometheus-community/yet-another-cloudwatch-exporter/pkg/clients/tagging"
	"github.com/prometheus-community/yet-another-cloudwatch-exporter/pkg/model"
	"github.com/prometheus-community/yet-another-cloudwatch-exporter/pkg/resourceinventory"
)

type fakeResourceSource struct {
	resources []*model.TaggedResource
	err       error
}

func (f fakeResourceSource) GetResources(_ context.Context, _ model.DiscoveryJob, _ string) ([]*model.TaggedResource, error) {
	return f.resources, f.err
}

func TestWithExternalStore_GetResources(t *testing.T) {
	errAccessDenied := errors.New("access denied")
	storeResource := &model.TaggedResource{ARN: "arn:aws:sns:us-east-1:123456789012:from-store"}

	tests := []struct {
		name          string
		primary       fakeResourceSource
		store         resourceinventory.Store
		wantErr       error
		wantResources []*model.TaggedResource
	}{
		{
			name:    "search tags match nothing and there is no external store",
			primary: fakeResourceSource{err: tagging.ErrExpectedToFindResources},
			store:   nil,
			wantErr: tagging.ErrExpectedToFindResources,
		},
		{
			name:    "search tags match nothing and the external store is empty",
			primary: fakeResourceSource{err: tagging.ErrExpectedToFindResources},
			store:   fakeResourceSource{},
			wantErr: tagging.ErrExpectedToFindResources,
		},
		{
			name:    "primary fails and the external store is empty",
			primary: fakeResourceSource{err: errAccessDenied},
			store:   fakeResourceSource{},
			wantErr: errAccessDenied,
		},
		{
			name:          "primary finds nothing but the external store has resources",
			primary:       fakeResourceSource{err: tagging.ErrExpectedToFindResources},
			store:         fakeResourceSource{resources: []*model.TaggedResource{storeResource}},
			wantResources: []*model.TaggedResource{storeResource},
		},
		{
			name:          "namespace without resource filters returns an empty list",
			primary:       fakeResourceSource{},
			store:         nil,
			wantResources: []*model.TaggedResource{},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			client := tagging.WithExternalStore(slog.New(slog.DiscardHandler), tc.primary, tc.store)

			resources, err := client.GetResources(context.Background(), model.DiscoveryJob{Namespace: "AWS/SNS"}, "us-east-1")

			if tc.wantErr != nil {
				require.ErrorIs(t, err, tc.wantErr)
				require.Nil(t, resources)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.wantResources, resources)
		})
	}
}
