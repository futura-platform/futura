package ftype

type MomentFnMetadata struct {
	Label string
}

type MomentFnOption func(*MomentFnMetadata)

func WithLabel(label string) MomentFnOption {
	return func(m *MomentFnMetadata) {
		m.Label = label
	}
}
