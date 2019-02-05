// Copyright 2018 Datawire. All rights reserved.

package k8sutil

import (
	"context"
	"net/http"
	"reflect"

	"github.com/ericchiang/k8s"
	"github.com/pkg/errors"
)

// Logger describes what k8sutil utilities expect from a logger.  Any
// logger implementation that provides these methods may be used with
// k8sutil.
type Logger interface {
	Errorf(format string, args ...interface{})
}

func getResourceListItems(list k8s.ResourceList) []k8s.Resource {
	sliceValue := reflect.ValueOf(list).FieldByName("Items")
	ret := make([]k8s.Resource, sliceValue.Len())
	for i := 0; i < len(ret); i++ {
		ret[i] = sliceValue.Index(i).Interface().(k8s.Resource)
	}
	return ret
}

func getNewResourceInstance(x k8s.Resource) k8s.Resource {
	return reflect.New(reflect.TypeOf(x).Elem()).Interface().(k8s.Resource)
}

func getNewResourceListInstance(x k8s.ResourceList) k8s.ResourceList {
	return reflect.New(reflect.TypeOf(x).Elem()).Interface().(k8s.ResourceList)
}

type watchEvent struct {
	eventType string
	resource  k8s.Resource
}

type watch struct {
	namespace    string
	resource     k8s.Resource
	resourceList k8s.ResourceList
}

func newWatch(namespace string, resourceList k8s.ResourceList) watch {
	listType := reflect.TypeOf(resourceList)
	if listType.Kind() != reflect.Ptr {
		panic(errors.Errorf("k8s.ResourceList type %s isn't a pointer", listType))
	}
	itemsField, ok := listType.FieldByName("Items")
	if !ok {
		panic(errors.Errorf("k8s.ResourceList type %s doesn't have a .Items field", listType))
	}
	if itemsField.Type.Kind() != reflect.Slice {
		panic(errors.Errorf("k8s.ResourceList type %s .Items isn't a slice", listType))
	}
	itemType := itemsField.Type.Elem()
	if !itemType.Implements(reflect.TypeOf((*k8s.Resource)(nil)).Elem()) {
		panic(errors.Errorf("k8s.ResourceList type %s member %s doesn't implement k8s.Resource", listType, itemType))
	}
	if itemType.Kind() != reflect.Ptr {
		panic(errors.Errorf("k8s.ResourceList type %s member %s isn't a pointer", listType, itemType))
	}

	return watch{
		namespace:    namespace,
		resource:     reflect.New(itemType).Interface().(k8s.Resource),
		resourceList: reflect.New(listType).Interface().(k8s.ResourceList),
	}
}

func (w watch) run(ctx context.Context, client *k8s.Client, logger Logger,
	listCh chan<- []k8s.Resource, watchCh chan<- watchEvent) {

	var resourceVersion string
	for {
		if ctx.Err() != nil {
			return
		}
		list := getNewResourceListInstance(w.resourceList)
		if err := client.List(ctx, w.namespace, list); err != nil {
			logger.Errorf("list %s (namespace=%q): %v", reflect.TypeOf(w.resource), w.namespace, err)
			continue
		}
		resourceVersion = list.GetMetadata().GetResourceVersion()
		listCh <- getResourceListItems(list)
		break
	}
	for {
		if ctx.Err() != nil {
			return
		}
		watcher, err := client.Watch(ctx, w.namespace, getNewResourceInstance(w.resource),
			k8s.ResourceVersion(resourceVersion))
		if err != nil {
			logger.Errorf("create %s (namespace=%q) watch: %v", reflect.TypeOf(w.resource), w.namespace, err)
			if apiErr, ok := err.(*k8s.APIError); ok && apiErr.Code == http.StatusGone {
				return
			}
			continue
		}
		for {
			resource := getNewResourceInstance(w.resource)
			eventType, err := watcher.Next(resource)
			if err != nil {
				logger.Errorf("read %s (namespace=%q) watch: %v", reflect.TypeOf(w.resource), w.namespace, err)
				_ = watcher.Close()
				if apiErr, ok := err.(*k8s.APIError); ok && apiErr.Code == http.StatusGone {
					return
				}
				break
			}
			resourceVersion = resource.GetMetadata().GetResourceVersion()
			watchCh <- watchEvent{eventType, resource}
		}
	}
}
