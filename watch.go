package k8sutil

import (
	"context"
	"reflect"

	"github.com/ericchiang/k8s"
	"github.com/pkg/errors"
)

type Logger interface {
	Errorf(args ...interface{})
}

type WatchingStore struct {
	client   *k8s.Client
	logger   Logger
	callback func(Store)

	watches []watch
	store   map[reflect.Type]map[string]k8s.Resource
}

type Store interface {
	List(resourceType k8s.Resource) []k8s.Resource
}

type mapStore map[reflect.Type]map[string]k8s.Resource

func (store mapStore) List(resourceType k8s.Resource) []k8s.Resource {
	rt := reflect.TypeOf(resourceType)
	ret := make([]k8s.Resource, 0, len(store[rt]))
	for _, resource := range store[rt] {
		ret = append(ret, resource)
	}
	return ret
}

func NewWatchingStore(client *k8s.Client, logger Logger, callback func(Store)) *WatchingStore {
	return &WatchingStore{
		client:   client,
		logger:   logger,
		store:    map[reflect.Type]map[string]k8s.Resource{},
		callback: callback,
	}
}

func (w *WatchingStore) notify() {
	w.callback(mapStore(w.store))
}

func (w *WatchingStore) AddWatch(namespace string, resourceList k8s.ResourceList) {
	w.watches = append(w.watches, newWatch(namespace, resourceList))
}

func (w *WatchingStore) Run(ctx context.Context) error {
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		w.run(ctx)
	}
}

func (w *WatchingStore) run(ctx context.Context) {
	ctx, cancelCtx := context.WithCancel(ctx)

	listCh := make(chan []k8s.Resource)
	listCnt := 0

	watchCh := make(chan watchEvent)

	exitCh := make(chan struct{})
	exitCnt := 0

	for _, watch := range w.watches {
		go func() {
			watch.run(ctx, w.client, w.logger, listCh, watchCh)
			exitCh <- struct{}{}
		}()
	}

	dirty := false
	newUids := map[reflect.Type]map[string]struct{}{}
	for _, watch := range w.watches {
		rt := reflect.TypeOf(watch.resource)
		newUids[rt] = map[string]struct{}{}
		if _, ok := w.store[rt]; !ok {
			w.store[rt] = map[string]k8s.Resource{}
			dirty = true
		}
	}
	for {
		select {
		case list := <-listCh:
			for _, newResource := range list {
				rt := reflect.TypeOf(newResource)
				uid := newResource.GetMetadata().GetUid()
				newUids[rt][uid] = struct{}{}

				oldResource, existed := w.store[rt][uid]
				if !existed || oldResource.GetMetadata().GetResourceVersion() != newResource.GetMetadata().GetResourceVersion() {
					w.store[rt][uid] = newResource
					dirty = true
				}
			}

			listCnt++
			if listCnt == len(w.watches) {
				break
			}
		case <-exitCh:
			cancelCtx()
			exitCnt++
			if exitCnt == len(w.watches) {
				return
			}
		}
	}
	for rt := range w.store {
		for uid := range w.store[rt] {
			if _, ok := newUids[rt][uid]; !ok {
				delete(w.store[rt], uid)
				dirty = true
			}
		}
	}
	if dirty {
		w.notify()
	}

	for {
		select {
		case event := <-watchCh:
			newResource := event.resource
			rt := reflect.TypeOf(newResource)
			uid := newResource.GetMetadata().GetUid()

			switch event.eventType {
			case k8s.EventDeleted:
				_, existed := w.store[rt][uid]
				delete(w.store[rt], uid)
				if existed {
					w.notify()
				}
			case k8s.EventAdded, k8s.EventModified:
				oldResource, _ := w.store[rt][uid]
				if oldResource.GetMetadata().ResourceVersion != newResource.GetMetadata().ResourceVersion {
					w.store[rt][uid] = newResource
					w.notify()
				}
			default:
				panic(errors.Errorf("unexpected watch event type: %s", event.eventType))
			}
		case <-exitCh:
			cancelCtx()
			exitCnt++
			if exitCnt == len(w.watches) {
				return
			}
		}
	}
}
