package agreementbot

import (
	"errors"
	"fmt"
	"github.com/golang/glog"
	"github.com/open-horizon/anax/agreementbot/persistence"
	"github.com/open-horizon/anax/compcheck"
	"github.com/open-horizon/anax/config"
	"github.com/open-horizon/anax/cutil"
	"github.com/open-horizon/anax/exchange"
	"github.com/open-horizon/anax/policy"
	"github.com/open-horizon/anax/semanticversion"
	"github.com/open-horizon/edge-sync-service/common"
	"strings"
)

// This function is called when an object is ready to be deployed to a node. It will perform the policy compatibility test
// if necessary and will then update the object's destination list in the CSS.
func AssignObjectToNode(ec exchange.ExchangeContext, objPolicies *exchange.ObjectDestinationPolicies, nodeId string, nodePolicy *policy.Policy, knownCompatible bool) (bool, error) {

	if len(*objPolicies) == 0 {
		return false, nil
	}

	updateDestHandler := exchange.GetHTTPUpdateObjectDestinationHandler(ec)
	getObjectHandler := exchange.GetHTTPObjectQueryHandler(ec)
	objDestHandler := exchange.GetHTTPObjectDestinationQueryHandler(ec)

	currentObjDestinations := new(exchange.ObjectDestinationStatuses)

	// For each object policy received, make sure the object is still valid, evaluate it against the node policy if necessary,
	// and then update the object's destination list.
	for _, objPol := range *objPolicies {

		if obj, err := getObjectHandler(objPol.OrgID, objPol.ObjectID, objPol.ObjectType); err != nil {
			glog.Errorf(opLogstring(fmt.Sprintf("error reading object %v %v %v, %v", objPol.OrgID, objPol.ObjectID, objPol.ObjectType, err)))
		} else if obj == nil {
			glog.Warningf(opLogstring(fmt.Sprintf("object %v %v %v has been deleted", objPol.OrgID, objPol.ObjectID, objPol.ObjectType)))
			continue
		}

		// The caller might have already done the compatibility test.
		if !knownCompatible {
			glog.V(5).Infof(opLogstring(fmt.Sprintf("evaluating policy for object %v of type %v", objPol.ObjectID, objPol.ObjectType)))

			// Evaluate the object policy against the edge node policy. If the object policy is compatible, then place the object
			// on the node for the current agreement.

			// Convert the object's policy into an internal policy so that we can do the compatibility check.
			internalObjPol := policy.Policy_Factory(fmt.Sprintf("object policy for %v type %v", objPol.ObjectID, objPol.ObjectType))
			internalObjPol.Properties = objPol.DestinationPolicy.Properties
			internalObjPol.Constraints = objPol.DestinationPolicy.Constraints
			glog.V(5).Infof(opLogstring(fmt.Sprintf("converted object policy to: %v", internalObjPol)))

			// temporary fix - eliminate node constraints so that models can be deployed without repeating business policy
			// properties plus service policy properties in the model policy properties.
			nodePolicy.Constraints = []string{}

			// Check if node and model polices are compatible. Incompatible policies are not necessarily an error so just log a warning and return.
			if err := policy.Are_Compatible(nodePolicy, internalObjPol, nil); err != nil {
				glog.Warningf(opLogstring(fmt.Sprintf("error matching node policy %v and object policy %v, error: %v", nodePolicy, internalObjPol, err)))
				return false, nil
			} else {
				glog.V(3).Infof(opLogstring(fmt.Sprintf("node %v is compatible with object %v/%v with type %v", nodeId, objPol.OrgID, objPol.ObjectID, objPol.ObjectType)))
			}
		}

		// Grab the current destinations of the object.
		if dests, err := objDestHandler(objPol.OrgID, objPol.ObjectID, objPol.ObjectType); err != nil {
			glog.Errorf(opLogstring(fmt.Sprintf("error reading object %v %v %v destinations, %v", objPol.OrgID, objPol.ObjectID, objPol.ObjectType, err)))
		} else if dests != nil {
			currentObjDestinations = dests
		}

		// Policies are compatible so place this object on the node. If the node is not in
		// the destination list of the object, add it.
		pdlr := new(exchange.PutDestinationListRequest)
		found := false
		for _, destStatus := range *currentObjDestinations {
			if destStatus.DestID == exchange.GetId(nodeId) {
				// Found it, no need to update the destination list.
				found = true
				break
			} else {
				// The destination list update is a full replace so we have to capture all the current destinations as
				// we iterate the current list.
				(*pdlr) = append((*pdlr), destStatus.DestType+":"+destStatus.DestID)
			}
		}

		if !found {
			(*pdlr) = append((*pdlr), "openhorizon.edgenode:"+exchange.GetId(nodeId))

			// The update could fail if the object has been deleted in this small window.
			if err := updateDestHandler(objPol.OrgID, &objPol, pdlr); err != nil {
				glog.Warningf(opLogstring(fmt.Sprintf("failed to update object %v %v %v destination list, error %v", objPol.OrgID, objPol.ObjectID, objPol.ObjectType, err)))
			} else {
				glog.V(3).Infof(opLogstring(fmt.Sprintf("updated destination list for object %v of type %v with node %v", objPol.ObjectID, objPol.ObjectType, nodeId)))
			}
		} else {
			glog.V(5).Infof(opLogstring(fmt.Sprintf("node %v is already a destination for object %v with type %v", nodeId, objPol.ObjectID, objPol.ObjectType)))
		}
	}
	return true, nil
}

// This function is called to remove an object from a node. It is assumed that the caller has already done the
// policy compatibility check.
func UnassignObjectFromNode(ec exchange.ExchangeContext, objPol *exchange.ObjectDestinationPolicy, nodeId string) error {

	glog.V(5).Infof(opLogstring(fmt.Sprintf("unassign object %v %v %v from node %v", objPol.OrgID, objPol.ObjectType, objPol.ObjectID, nodeId)))

	updateDestHandler := exchange.GetHTTPUpdateObjectDestinationHandler(ec)
	getObjectHandler := exchange.GetHTTPObjectQueryHandler(ec)
	pdlr := new(exchange.PutDestinationListRequest)

	found := false
	for _, destStatus := range objPol.Destinations {
		if destStatus.DestID == exchange.GetId(nodeId) {
			found = true
		} else {
			// The destination list update is a full replace so we have to capture all the current destinations as
			// we iterate the current list.
			(*pdlr) = append((*pdlr), destStatus.DestType+":"+destStatus.DestID)
		}
	}

	glog.V(5).Infof(opLogstring(fmt.Sprintf("new destination list %v", *pdlr)))

	if found {
		// The update could fail if the object has been deleted. That should be treated as an expected error.
		if obj, err := getObjectHandler(objPol.OrgID, objPol.ObjectID, objPol.ObjectType); err != nil {
			glog.Errorf(opLogstring(fmt.Sprintf("object %v %v %v destination cannot be updated, %v", objPol.OrgID, objPol.ObjectID, objPol.ObjectType, err)))
		} else if obj == nil {
			glog.Warningf(opLogstring(fmt.Sprintf("object %v %v %v has been deleted", objPol.OrgID, objPol.ObjectID, objPol.ObjectType)))
		} else if err := updateDestHandler(objPol.OrgID, objPol, pdlr); err != nil {
			glog.Errorf(opLogstring(fmt.Sprintf("%v", err)))
		} else {
			glog.V(3).Infof(opLogstring(fmt.Sprintf("updated destination list for object %v/%v of type %v to remove node %v", objPol.OrgID, objPol.ObjectID, objPol.ObjectType, nodeId)))
		}
	}
	return nil
}

// MMS object policy changes can cause a significant impact to where objects are placed throughout the entire system.
// Any aspect of the policy could have changed.
// When an MMS object policy changes, it might mean one of the following:
// 1. Nothing changes.
//   a. A brand new policy is not eligible for any node on which the referenced service(s) are already running.
//   b. A policy change is still not sufficent to make the object eligible for nodes that are running the referenced service(s).
// 2. There are nodes from which the object should be removed.
// 3. There are nodes on which the object should be placed, where the referenced service(s) are running.
//   a. A new object/policy is placed on the node long after the agreement is in place.
//   b. A policy change makes the object eligible for the node long after the agreement is in place.
//
// Objects are not placed on nodes without an agreement, so we can find all the relevant nodes by looking through
// all of our agreements. The actions we can take are to either add or remove a node from the object's destination list.
//
func (w *BaseAgreementWorker) HandleMMSObjectPolicy(cph ConsumerProtocolHandler, wi *ObjectPolicyChange, workerId string) {

	glog.V(3).Infof(BAWlogstring(workerId, fmt.Sprintf("received MMS Object Policy event: %v", wi)))

	// Convert the object policies in the message to their real types.
	var oldPolicy exchange.ObjectDestinationPolicy
	var newPolicy exchange.ObjectDestinationPolicy
	var ok bool

	if wi.Event.OldPolicy != nil {
		if oldPolicy, ok = wi.Event.OldPolicy.(exchange.ObjectDestinationPolicy); !ok {
			glog.Errorf(BAWlogstring(workerId, fmt.Sprintf("Object Policy event contains incorrect old policy type (%T)", wi.Event.OldPolicy)))
			return
		}
	}

	if wi.Event.NewPolicy == nil {
		glog.Errorf(BAWlogstring(workerId, fmt.Sprintf("Object Policy event missing new policy")))
		return
	} else if newPolicy, ok = wi.Event.NewPolicy.(exchange.ObjectDestinationPolicy); !ok {
		glog.Errorf(BAWlogstring(workerId, fmt.Sprintf("Object Policy event contains incorrect new policy type (%T)", wi.Event.NewPolicy)))
		return
	}

	glog.V(5).Infof(BAWlogstring(workerId, fmt.Sprintf("Object Policy OldPolicy: %v", oldPolicy)))
	glog.V(5).Infof(BAWlogstring(workerId, fmt.Sprintf("Object Policy NewPolicy: %v", newPolicy)))

	// Construct a list of destinations where the object currently lives. These will be in the policy update (the new policy).
	destNodes := make([]string, 0, 5)
	for _, dest := range newPolicy.Destinations {
		destNodes = append(destNodes, dest.DestID)
	}

	glog.V(5).Infof(BAWlogstring(workerId, fmt.Sprintf("Object Policy current dest nodes: %v", destNodes)))

	objPolicies := new(exchange.ObjectDestinationPolicies)
	(*objPolicies) = append((*objPolicies), newPolicy)

	// Find all policy related agreements that are in progress. Agreements are scanned in pages so that we dont consume too much memory.
	nextAgreementId := ""
	for {
		lastAgreementId, agreements, err := w.db.FindAgreementsPage([]persistence.AgbotDBFilter{w.db.GetActiveFilter(), w.db.GetNoPatternFilter(), w.db.GetUnarchivedFilter()}, cph.Name(), nextAgreementId, w.config.GetAgbotDBLimit())
		if err != nil {
			glog.Errorf(BAWlogstring(workerId, fmt.Sprintf("Object Policy unable to read agreements, error %v", err)))
			return
		}

		// If there are no more agreements to iterate, then break out of the loop.
		if lastAgreementId == "" {
			break
		} else {
			nextAgreementId = lastAgreementId
		}

		// The main logic in the function can be summarized as follows. The top half of the algorithm verifies that any policy changes
		// in the object's constraints are checked. The bottom half of the algorithm verifies that changes to the object policy's
		// service list are handled correctly.
		//
		// for all agreements in this agbot: (this ensures that the agbot only considers agreements in it's scope)
		//   if the agreement is for a service that is compatible (including arch and version range) with a service in the new policy
		//     if agreement's node's policy is compatible with new object policy
		//       if agreement's node is NOT in current obj dest list, then
		//         add the agreement's node to object's destination list
		//       else
		//         nothing to do, the object is already on the agreement's node
		//     else
		//       if agreement's node is in the object's destination list, then
		//         remove the agreement's node from obj destination list
		//       else
		//         nothing to do, the agreement's node is not in the object's destination list

		//   else (we might have to remove the node from the object's destination list - if the object policy's service list changed)
		//     if the policy change event includes an old/previous Policy (which is the policy before the change)
		//       if the old Policy's service list is different from newPolicy service list (then a service list change has occurred, so more checks are required)
		//         if the agreement is for a service that is compatible with a service in the old policy (this agreement's node might need to be removed from the object's destination list)
		//           if the agreement's node is in current object's destination list, then (it needs to be removed if there are no other services on the node which are compatible with the object policy)
		//             find ALL the services running on the node (even the services for which this agbot doesnt have an agreement)
		//             if none of them are in new policy then
		//               remove the node from the dest list
		//             else
		//               nothing to do, assume that the agbot which owns the agreement for the other services will handle this same policy change event appropriately
		//           else
		//             nothing to do, node is not in object's destination list
		//         else
		//           nothing to do, this agreement is irrelevant because it contains services in neither the old nor new object policy
		//       else (the new policy service list has not changed, and since it doesnt match a service in this agreement, there is nothing to do)
		//         nothing to do
		//     else (no old policy so the object policy's service list hasnt changed)
		//       nothing to do, the new policy didn't match any services of agreements owned by this agbot.

		for _, agreement := range agreements {

			// if the agreement is for a service that is compatible (including arch and version range) with a service in the new policy
			if w.findCompatibleServices(&agreement, &newPolicy, workerId, w.config.ArchSynonyms) {

				_, nodePolicy, err := compcheck.GetNodePolicy(exchange.GetHTTPNodePolicyHandler(w), agreement.DeviceId, nil)

				if err != nil {
					glog.Errorf(BAWlogstring(workerId, fmt.Sprintf("Object Policy error %v", err)))
				} else if nodePolicy == nil {
					glog.Errorf(BAWlogstring(workerId, fmt.Errorf("No node policy found for %v", agreement.DeviceId)))
				} else {

					// if agreement's node's policy is compatible with new object policy
					//   if agreement's node is NOT in current obj dest list, then
					//     add the agreement's node to object's destination list
					added, err := AssignObjectToNode(w, objPolicies, agreement.DeviceId, nodePolicy, false)
					if err != nil {
						glog.Errorf(BAWlogstring(workerId, fmt.Sprintf("Object Policy error %v", err)))
					} else if !added {
						//  else
						//    if agreement's node is in the object's destination list, then
						//      remove the agreement's node from obj destination list
						err := UnassignObjectFromNode(w, &newPolicy, agreement.DeviceId)
						if err != nil {
							glog.Errorf(BAWlogstring(workerId, fmt.Sprintf("Object Policy error %v", err)))
						}
					}
				}
				continue
			}

			// else (we might have to remove the node from the object's destination list - if the object policy's service list changed)
			//   if the policy change event includes an old/previous Policy (which is the policy before the change)
			if wi.Event.OldPolicy != nil {

				// if the old Policy's service list is different from newPolicy service list (then a service list change has occurred, so more checks are required)
				if hasDifferentServiceLists(&newPolicy, &oldPolicy) {

					// if the agreement is for a service that is compatible with a service in the old policy (this agreement's node might need to be removed
					// from the object's destination list)
					if w.findCompatibleServices(&agreement, &oldPolicy, workerId, w.config.ArchSynonyms) {

						// if the agreement's node is in current object's destination list, then (it needs to be removed if there are no other services on
						// the node which are compatible with the object policy)
						if cutil.SliceContains(destNodes, exchange.GetId(agreement.DeviceId)) {

							// find ALL the services running on the node (even the services for which this agbot doesnt have an agreement)
							ns, err := exchange.GetHTTPNodeStatusHandler(w)(agreement.DeviceId)
							if err != nil {
								glog.Errorf(BAWlogstring(workerId, fmt.Sprintf("Object Policy unable to get node status, error %v", err)))
								continue
							}
							glog.V(3).Infof(BAWlogstring(workerId, fmt.Sprintf("Object Policy got node status: %v", ns.RunningServices)))

							// if none of them are in new policy then
							if !hasRunningService(ns.RunningServices, &newPolicy, workerId, w.config.ArchSynonyms) {
								//   remove the node from the dest list
								err := UnassignObjectFromNode(w, &newPolicy, agreement.DeviceId)
								if err != nil {
									glog.Errorf(BAWlogstring(workerId, fmt.Sprintf("Object Policy error %v", err)))
								}
							} else {
								// else
								//   nothing to do, assume that the agbot which owns the agreement for the other services will handle this same policy change event appropriately
							}
						} else {
							// else
							//   nothing to do, node is not in object's destination list
						}
					} else {
						// else
						//   nothing to do, this agreement is irrelevant because it contains services in neither the old nor new object policy
					}
				} else {
					// else (the new policy service list has not changed, and since it doesnt match a service in this agreement, there is nothing to do)
					//   nothing to do
				}
			} else {
				// else (no old policy so the object policy's service list hasnt changed)
				//   nothing to do, the new policy didn't match any services of agreements owned by this agbot.
			}

		}
	}

	glog.V(3).Infof(BAWlogstring(workerId, fmt.Sprintf("done with MMS Object Policy event: %v", wi)))

	return

}

// This function encapsulates the search of all services in an agreement.
func (w *BaseAgreementWorker) findCompatibleServices(agreement *persistence.Agreement, objPol *exchange.ObjectDestinationPolicy, workerId string, archSynonyms config.ArchSynonyms) bool {
	for _, serviceId := range agreement.ServiceId {
		if foundService, err := findCompatibleService(serviceId, objPol, workerId, archSynonyms); err != nil {
			// findCompatibleService logs it own errors.
			continue
		} else if foundService {
			return true
		}
	}
	return false
}

// This function returns true if the input agreement service id is compatible with one of the service IDs
// in the object's policy.
func findCompatibleService(agreementServiceID string, objPol *exchange.ObjectDestinationPolicy, workerId string, archSynonyms config.ArchSynonyms) (bool, error) {

	// Break the service id into the individual tuple pieces, service name (which includes org), arch and version.
	agServiceIdPieces := strings.SplitN(agreementServiceID, "_", 3)

	// Separate the service name and org.
	agServiceNamePieces := strings.SplitN(agServiceIdPieces[0], "/", 2)

	// For each service ID in the object policy, check to see if this agreement is using a service that is compatible
	// with it. If so, we need to add this object to the agreement's node.
	found := false
	for _, objPolServiceID := range objPol.DestinationPolicy.Services {

		// If the service names and orgs match, then the object might be compatible. Just need to verify the arch and
		// version ranges.
		if objPolServiceID.ServiceName == agServiceNamePieces[1] && objPolServiceID.OrgID == agServiceNamePieces[0] {

			glog.V(3).Infof(BAWlogstring(workerId, fmt.Sprintf("Object Policy found agreement's service in policy")))

			// Make sure the object policy Arch is compatible with the arch in the agreement's service id.
			if ok := SupportsArch(&objPolServiceID, agServiceIdPieces[2], archSynonyms); !ok {
				glog.V(3).Infof(BAWlogstring(workerId, fmt.Sprintf("Object Policy rejecting for arch")))
				continue
			}

			// Make sure the agreement's service id is within the object policy's version reange.
			if ok, err := SupportsVersion(&objPolServiceID, agServiceIdPieces[1]); err != nil {
				glog.Errorf(BAWlogstring(workerId, fmt.Sprintf("Object Policy for %v %v %v, error checking version compatibility, %v", objPol.OrgID, objPol.ObjectID, objPol.ObjectType, err)))
				continue
			} else if !ok {
				glog.V(3).Infof(BAWlogstring(workerId, fmt.Sprintf("Object Policy rejecting for version")))
				continue
			}

			// The Object Policy is compatible with the current agreement service id.
			found = true
			break

		}
	}
	return found, nil
}

func SupportsArch(objPolServiceID *common.ServiceID, serviceArch string, archSynonyms config.ArchSynonyms) bool {
	// Ths MMS API (CSS) doesnt support an empty arch. Use "*" to mean any arch.
	if objPolServiceID.Arch != "*" {
		canonicalArch := archSynonyms.GetCanonicalArch(objPolServiceID.Arch)
		return (canonicalArch != "" && canonicalArch == serviceArch) || objPolServiceID.Arch == serviceArch
	}
	return true
}

func SupportsVersion(objPolServiceID *common.ServiceID, serviceVersion string) (bool, error) {
	if versionExp, err := semanticversion.Version_Expression_Factory(objPolServiceID.Version); err != nil {
		return false, errors.New(fmt.Sprintf("unrecognized version expression %v, error %v", serviceVersion, err))
	} else if ok, err := versionExp.Is_within_range(serviceVersion); err != nil {
		return false, errors.New(fmt.Sprintf("unable to check version %v against range %v, error %v", serviceVersion, versionExp, err))
	} else {
		return ok, nil
	}
}

// Return true if the service list of both policies is the same.
func hasDifferentServiceLists(newPolicy *exchange.ObjectDestinationPolicy, oldPolicy *exchange.ObjectDestinationPolicy) bool {
	if len(newPolicy.DestinationPolicy.Services) != len(oldPolicy.DestinationPolicy.Services) {
		return true
	} else {
		for _, newServiceID := range newPolicy.DestinationPolicy.Services {
			found := false
			for _, oldServiceID := range oldPolicy.DestinationPolicy.Services {
				if newServiceID.ServiceName == oldServiceID.ServiceName && newServiceID.OrgID == oldServiceID.OrgID && newServiceID.Arch == oldServiceID.Arch && newServiceID.Version == oldServiceID.Version {
					found = true
					break
				}
			}
			if !found {
				return true
			}
		}
		return false
	}
}

// Return true if the input running services are in the object policy.
func hasRunningService(allRunningServices string, newPolicy *exchange.ObjectDestinationPolicy, workerId string, archSynonyms config.ArchSynonyms) bool {
	runningServices := strings.Split(allRunningServices, "|")
	for _, rs := range runningServices {
		if rs == "" {
			continue
		}
		servicePieces := strings.SplitN(rs, "/", 2)
		if len(servicePieces) < 2 {
			continue
		}
		prelimServiceName := servicePieces[1]
		prelimPieces := strings.Split(prelimServiceName, "_")
		if len(prelimPieces) < 3 {
			continue
		}
		finalServicename := cutil.FormExchangeIdWithSpecRef(prelimPieces[0])
		convertedRunningServiceName := fmt.Sprintf("%v/%v_%v_%v", servicePieces[0], finalServicename, prelimPieces[1], prelimPieces[2])

		foundService, err := findCompatibleService(convertedRunningServiceName, newPolicy, workerId, archSynonyms)
		if err != nil {
			continue
		} else if foundService {
			return true
		}
	}
	return false
}

// =============================================================================================================
var opLogstring = func(v interface{}) string {
	return fmt.Sprintf("Object Policy: %v", v)
}
