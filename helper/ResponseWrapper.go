package helper

type ResponserWrapper struct {
	Resource []interface{} `json:"data"`
}

func (receiver *ResponserWrapper) SetResource(entity interface{}) *ResponserWrapper {
	transformers := append(receiver.Resource, entity)
	receiver.Resource = transformers
	return receiver
}

func (receiver *ResponserWrapper) SetCollectionResource(e []interface{}) *ResponserWrapper {

	interfaces := make([]interface{}, len(e))
	for i, room := range e {
		interfaces[i] = room
	}
	receiver.Resource = interfaces
	return receiver
}

func (receiver *ResponserWrapper) GetResource() []interface{} {

	return receiver.Resource
}

func NewResponseWrapper() *ResponserWrapper {

	return &ResponserWrapper{}
}
