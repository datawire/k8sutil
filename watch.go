// Copyright 2018 Datawire. All rights reserved.

package k8sutil

import (
	"context"
	"reflect"

	"github.com/ericchiang/k8s"
	"github.com/pkg/errors"
)

// A Store allows you to query the stored state of the cluster.
type Store interface {
	// List returns all stored resources with the same type as the
	// given "sample" resource.  It is not valid to mutate any of
	// the resource returned.
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

// WatchingStore watches a set of resources (specified with
// .AddWatch() after creating the WatchingStore) and stores the
// current state of the cluster.
//
// The specified Callback will only be called when the store is in a
// "consistent" state.  It will not be called before a complete
// listing for each of the added watches has been added to the store.
//
// The Callback is called synchronously.  The Callback is not told
// what changed between callbacks, because there may be multiple
// changes that are coalesced.
type WatchingStore struct {
	Client   *k8s.Client // must not be nil
	Logger   Logger      // must not be nil
	Callback func(Store) // must not be nil

	watches []watch
	store   map[reflect.Type]map[string]k8s.Resource
}

func (w *WatchingStore) notify() {
	w.Callback(mapStore(w.store))
}

// AddWatch adds to the resources that the WatchingStore keeps track
// of.
//
// Use w.Client.Namespace to watch default namespace, or the special
// value k8s.AllNamespaces to watch namespaces.
//
// The ResourceList specifies the type of the resources to watch; the
// value itself is ignored.
//
// For example:
//
//     w.AddWatch(k8s.AllNamespaces, &corev1.PodList{})
//
// It is invalid to call .AddWatch() while .Run() is running.
func (w *WatchingStore) AddWatch(namespace string, resourceList k8s.ResourceList) {
	w.watches = append(w.watches, newWatch(namespace, resourceList))
}

// Run performs the initial list calls to populate the store, and then
// launches the following watch calls to keep it up to date.
//
// It is invalid to call .AddWatch() while .Run() is running.
func (w *WatchingStore) Run(ctx context.Context) error {
	// The store is keyed by the resource type.  Because it is
	// possible to register multiple watches for that resource
	// type (in different namespaces), when one watch needs to
	// re-list because of a 410 response, we need to force that
	// for all watches, so that we don't miss delete events.  We
	// do that by killing all watches when 1 dies, and restarting
	// everything.
	//
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		w.run(ctx)
	}
}

// run performs 1 "round" of list+watch calls.  Once the first watch
// in this round dies, all others are canceled, so that they can all
// be restarted.  See the comment in Run().
func (w *WatchingStore) run(ctx context.Context) {
	ctx, cancelCtx := context.WithCancel(ctx)

	listCh := make(chan []k8s.Resource)
	listCnt := 0

	watchCh := make(chan watchEvent)

	exitCh := make(chan struct{})
	exitCnt := 0

	for _, watch := range w.watches {
		go func() {
			watch.run(ctx, w.Client, w.Logger, listCh, watchCh)
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
