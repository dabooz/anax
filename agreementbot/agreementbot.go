package agreementbot

import (
	"bytes"
	"errors"
	"fmt"
	"github.com/golang/glog"
	"github.com/open-horizon/anax/abstractprotocol"
	"github.com/open-horizon/anax/agreementbot/matchcache"
	"github.com/open-horizon/anax/agreementbot/persistence"
	"github.com/open-horizon/anax/config"
	"github.com/open-horizon/anax/cutil"
	"github.com/open-horizon/anax/events"
	"github.com/open-horizon/anax/exchange"
	"github.com/open-horizon/anax/externalpolicy"
	"github.com/open-horizon/anax/policy"
	"github.com/open-horizon/anax/version"
	"github.com/open-horizon/anax/worker"
	"net/http"
	"strconv"
	"strings"
	"time"
)

// for identifying the subworkers used by this worker
const DATABASE_HEARTBEAT = "AgbotDatabaseHeartBeat"
const GOVERN_AGREEMENTS = "AgBotGovernAgreements"
const GOVERN_ARCHIVED_AGREEMENTS = "AgBotGovernArchivedAgreements"

//const GOVERN_BC_NEEDS = "AgBotGovernBlockchain"
//const POLICY_WATCHER = "AgBotPolicyWatcher"
const STALE_PARTITIONS = "AgbotStaleDatabasePartition"
const MESSAGE_KEY_CHECK = "AgbotMessageKeyCheck"

// Agreement governance timing state. Used in the GovernAgreements subworker.
type DVState struct {
	dvSkip uint64
	nhSkip uint64
}

// must be safely-constructed!!
type AgreementBotWorker struct {
	worker.BaseWorker     // embedded field
	db                    persistence.AgbotDatabase
	httpClient            *http.Client // a shared HTTP client instance for this worker
	pm                    policy.IPolicyManager
	consumerPH            *ConsumerPHMgr
	ready                 bool
	PatternManager        *PatternManager
	BusinessPolManager    *BusinessPolicyManager
	NHManager             *NodeHealthManager
	GovTiming             DVState
	shutdownStarted       bool
	lastAgMakingTime      uint64 // the start time for the last agreement making cycle, only used by non-pattern case. TODO, not thread safe.
	MMSObjectPM           *MMSObjectPolicyManager
	lastSearchPolComplete bool
	searchPolThread       chan bool
	lastSearchPatComplete bool
	searchPatThread       chan bool
	lastSearchTime        uint64
	matchCache            *matchcache.MatchCache
	changedNodes          map[string]interface{} // A set of nodes that have changed recently.
}

func NewAgreementBotWorker(name string, cfg *config.HorizonConfig, db persistence.AgbotDatabase) *AgreementBotWorker {

	ec := worker.NewExchangeContext(cfg.AgreementBot.ExchangeId, cfg.AgreementBot.ExchangeToken, cfg.AgreementBot.ExchangeURL, cfg.AgreementBot.CSSURL, cfg.Collaborators.HTTPClientFactory)

	baseWorker := worker.NewBaseWorker(name, cfg, ec)
	worker := &AgreementBotWorker{
		BaseWorker:            baseWorker,
		db:                    db,
		httpClient:            cfg.Collaborators.HTTPClientFactory.NewHTTPClient(nil),
		consumerPH:            NewConsumerPHMgr(),
		ready:                 false,
		PatternManager:        NewPatternManager(),
		BusinessPolManager:    NewBusinessPolicyManager(),
		NHManager:             NewNodeHealthManager(),
		GovTiming:             DVState{},
		shutdownStarted:       false,
		lastAgMakingTime:      0,
		lastSearchPolComplete: true,
		searchPolThread:       make(chan bool, 10),
		lastSearchPatComplete: true,
		searchPatThread:       make(chan bool, 100),
		lastSearchTime:        0,
		MMSObjectPM:           NewMMSObjectPolicyManager(cfg),
		matchCache:            matchcache.NewMatchCache(),
		changedNodes:          make(map[string]interface{}),
	}

	worker.pm = NewAgreementbotPolicyManager(worker.BusinessPolManager, worker.PatternManager)

	glog.Info("Starting AgreementBot worker")
	worker.Start(worker, int(cfg.AgreementBot.NewContractIntervalS))
	return worker
}

func (w *AgreementBotWorker) ShutdownStarted() bool {
	return w.shutdownStarted
}

func (w *AgreementBotWorker) Messages() chan events.Message {
	return w.BaseWorker.Manager.Messages
}

func (w *AgreementBotWorker) NewEvent(incoming events.Message) {

	if w.Config.AgreementBot == (config.AGConfig{}) {
		return
	}

	switch incoming.(type) {
	case *events.AccountFundedMessage:
		msg, _ := incoming.(*events.AccountFundedMessage)
		switch msg.Event().Id {
		case events.ACCOUNT_FUNDED:
			cmd := NewAccountFundedCommand(msg)
			w.Commands <- cmd
		}

	case *events.BlockchainClientInitializedMessage:
		msg, _ := incoming.(*events.BlockchainClientInitializedMessage)
		switch msg.Event().Id {
		case events.BC_CLIENT_INITIALIZED:
			cmd := NewClientInitializedCommand(msg)
			w.Commands <- cmd
		}

	case *events.BlockchainClientStoppingMessage:
		msg, _ := incoming.(*events.BlockchainClientStoppingMessage)
		switch msg.Event().Id {
		case events.BC_CLIENT_STOPPING:
			cmd := NewClientStoppingCommand(msg)
			w.Commands <- cmd
		}

	case *events.EthBlockchainEventMessage:
		if w.ready {
			msg, _ := incoming.(*events.EthBlockchainEventMessage)
			switch msg.Event().Id {
			case events.BC_EVENT:
				agCmd := NewBlockchainEventCommand(*msg)
				w.Commands <- agCmd
			}
		}

	case *events.ABApiAgreementCancelationMessage:
		if w.ready {
			msg, _ := incoming.(*events.ABApiAgreementCancelationMessage)
			switch msg.Event().Id {
			case events.AGREEMENT_ENDED:
				agCmd := NewAgreementTimeoutCommand(msg.AgreementId, msg.AgreementProtocol, w.consumerPH.Get(msg.AgreementProtocol).GetTerminationCode(TERM_REASON_USER_REQUESTED))
				w.Commands <- agCmd
			}
		}

	// case *events.PolicyChangedMessage:
	// 	if w.ready {
	// 		msg, _ := incoming.(*events.PolicyChangedMessage)
	// 		switch msg.Event().Id {
	// 		case events.CHANGED_POLICY:
	// 			pcCmd := NewPolicyChangedCommand(*msg)
	// 			w.Commands <- pcCmd
	// 		}
	// 	}

	// case *events.PolicyDeletedMessage:
	// 	if w.ready {
	// 		msg, _ := incoming.(*events.PolicyDeletedMessage)
	// 		switch msg.Event().Id {
	// 		case events.DELETED_POLICY:
	// 			pdCmd := NewPolicyDeletedCommand(*msg)
	// 			w.Commands <- pdCmd
	// 		}
	// 	}

	case *events.ABApiWorkloadUpgradeMessage:
		if w.ready {
			msg, _ := incoming.(*events.ABApiWorkloadUpgradeMessage)
			switch msg.Event().Id {
			case events.WORKLOAD_UPGRADE:
				wuCmd := NewWorkloadUpgradeCommand(*msg)
				w.Commands <- wuCmd
			}
		}

	case *events.NodeShutdownCompleteMessage:
		msg, _ := incoming.(*events.NodeShutdownCompleteMessage)
		switch msg.Event().Id {
		case events.UNCONFIGURE_COMPLETE:
			w.Commands <- worker.NewBeginShutdownCommand()
			w.Commands <- worker.NewTerminateCommand("shutdown")
		case events.AGBOT_QUIESCE_COMPLETE:
			w.Commands <- worker.NewTerminateCommand("shutdown")
		}

	case *events.NodeShutdownMessage:
		msg, _ := incoming.(*events.NodeShutdownMessage)
		switch msg.Event().Id {
		case events.START_AGBOT_QUIESCE:
			w.Commands <- NewAgbotShutdownCommand(msg)
		}

	// case *events.CacheServicePolicyMessage:
	// 	msg, _ := incoming.(*events.CacheServicePolicyMessage)

	// 	switch msg.Event().Id {
	// 	case events.CACHE_SERVICE_POLICY:
	// 		w.Commands <- NewCacheServicePolicyCommand(msg)
	// 	}

	// case *events.ServicePolicyChangedMessage:
	// 	msg, _ := incoming.(*events.ServicePolicyChangedMessage)
	// 	switch msg.Event().Id {
	// 	case events.SERVICE_POLICY_CHANGED:
	// 		w.Commands <- NewServicePolicyChangedCommand(msg)
	// 	}

	// case *events.ServicePolicyDeletedMessage:
	// 	msg, _ := incoming.(*events.ServicePolicyDeletedMessage)
	// 	switch msg.Event().Id {
	// 	case events.SERVICE_POLICY_DELETED:
	// 		w.Commands <- NewServicePolicyDeletedCommand(msg)
	// 	}

	case *events.MMSObjectPolicyMessage:
		// This is an event from the object manager indicating a specific object policy change. These are
		// changes that require an agot worker to analyze because an object routing change is likely to be
		// needed.
		msg, _ := incoming.(*events.MMSObjectPolicyMessage)
		w.Commands <- NewMMSObjectPolicyEventCommand(msg)

	case *events.MMSObjectPoliciesMessage:
		// This is an event from the changes worker indicating there are some object policy changes to process.
		// The changes are given to the object manager to process and cache. It will emit more events if there
		// are changes in object routing that need to be made.
		msg, _ := incoming.(*events.MMSObjectPoliciesMessage)
		w.Commands <- NewObjectPoliciesChangeCommand(msg)

	case *events.ExchangeChangeMessage:
		msg, _ := incoming.(*events.ExchangeChangeMessage)
		switch msg.Event().Id {
		case events.CHANGE_AGBOT_MESSAGE_TYPE:
			w.Commands <- NewMessageCommand(msg)
		case events.CHANGE_AGBOT_SERVED_PATTERN:
			w.Commands <- NewServedPatternCommand()
		case events.CHANGE_AGBOT_SERVED_POLICY:
			w.Commands <- NewServedPolicyCommand()
		case events.CHANGE_AGBOT_PATTERN:
			w.Commands <- NewPatternChangeCommand(msg)
		case events.CHANGE_AGBOT_POLICY:
			// A deployment policy has changed.
			w.Commands <- NewPolicyChangeCommand(msg)
		case events.CHANGE_SERVICE_POLICY_TYPE:
			w.Commands <- NewServicePolicyChangeCommand(msg)
		case events.CHANGE_NODE_POLICY_TYPE:
			// A node policy has changed.
			w.Commands <- NewNodePolicyChangeCommand(msg)
		case events.CHANGE_NODE_TYPE:
			// The node itself has changed, which includes node agreement changes.
			w.Commands <- NewNodeChangeCommand(msg)
		}

	default: //nothing

	}

	return
}

// This function is used by Initialize to send the Agbot terminate message in the cases where Initialize fails such that the
// entire agbot process should also terminate.
func (w *AgreementBotWorker) fail() bool {
	w.Messages() <- events.NewNodeShutdownCompleteMessage(events.AGBOT_QUIESCE_COMPLETE, "")
	return false
}

func (w *AgreementBotWorker) Initialize() bool {

	glog.Info("AgreementBot worker initializing")

	// If there is no Agbot config, we will terminate. This is a normal condition when running on a node.
	if w.Config.AgreementBot == (config.AGConfig{}) {
		glog.Warningf("AgreementBotWorker terminating, no AgreementBot config.")
		return false
	} else if w.db == nil {
		glog.Warningf("AgreementBotWorker terminating, no AgreementBot database configured.")
		return false
	}

	// Log an error if the current exchange version does not meet the requirement.
	if err := version.VerifyExchangeVersion(w.Config.Collaborators.HTTPClientFactory, w.GetExchangeURL(), w.GetExchangeId(), w.GetExchangeToken(), false); err != nil {
		glog.Errorf(AWlogString(fmt.Sprintf("Error verifiying exchange version. error: %v", err)))
		return w.fail()
	}

	// // TODO: Remove this
	// // Make sure the policy directory is in place so that we have a place to put the generated policy files.
	// if err := os.MkdirAll(w.BaseWorker.Manager.Config.AgreementBot.PolicyPath, 0644); err != nil {
	// 	glog.Errorf("AgreementBotWorker cannot create agreement bot policy file path %v, terminating.", w.BaseWorker.Manager.Config.AgreementBot.PolicyPath)
	// 	return w.fail()
	// }

	// // TODO: Remove tihs
	// // To start clean, remove all left over pattern based policy files from the last time the agbot was started.
	// // This is only called once at the agbot start up time.
	// if err := policy.DeleteAllPolicyFiles(w.BaseWorker.Manager.Config.AgreementBot.PolicyPath, true); err != nil {
	// 	glog.Errorf("AgreementBotWorker cannot clean up pattern based policy files under %v. %v", w.BaseWorker.Manager.Config.AgreementBot.PolicyPath, err)
	// 	return w.fail()
	// }

	// Start the go thread that heartbeats to the database and checks for stale partitions.
	w.DispatchSubworker(DATABASE_HEARTBEAT, w.databaseHeartBeat, int(w.BaseWorker.Manager.Config.GetPartitionStale()/3), false)
	w.DispatchSubworker(STALE_PARTITIONS, w.stalePartitions, int(w.BaseWorker.Manager.Config.GetPartitionStale()), false)

	// Give the policy manager a chance to read in all the policies. The agbot worker will not proceed past this point
	// until it has some policies to work with.

	//	for {

	// Query the exchange for patterns that this agbot is supposed to serve. If an error
	// occurs, it will be ignored. The Agbot should not proceed out of initialization until it has at least 1 policy/pattern
	// that it can serve.

	// w.saveAgbotServedPatterns()
	// if err := w.getAllPatterns(); err != nil {
	// 	glog.Errorf(AWlogString(fmt.Sprintf("unable to retrieve patterns, error %v", err)))
	// }

	// let policy manager read it
	// if filePolManager, err := policy.Initialize(w.BaseWorker.Manager.Config.AgreementBot.PolicyPath, w.Config.ArchSynonyms, w.serviceResolver, true, false); err != nil {
	// 	glog.Errorf("AgreementBotWorker unable to initialize policy manager, error: %v", err)
	// } else {
	// 	// creating business policy cache and update the policy manager
	// 	w.pm = filePolManager
	// w.saveAgbotServedPolicies()
	// if err := w.getAllDeploymentPols(); err != nil {
	// 	glog.Errorf(AWlogString(fmt.Sprintf("unable to retrieve deployment policies, error %v", err)))
	// // } else if w.pm.NumberPolicies() != 0 {
	// // 	break
	// }
	// }
	// glog.V(3).Infof("AgreementBotWorker waiting for policies to appear")
	// time.Sleep(time.Duration(w.BaseWorker.Manager.Config.AgreementBot.CheckUpdatedPolicyS) * time.Second)

	// }

	glog.Info("AgreementBot worker started")

	// Make sure that our public key is registered in the exchange so that other parties
	// can send us messages.
	if err := w.registerPublicKey(); err != nil {
		glog.Errorf("AgreementBotWorker unable to register public key, error: %v", err)
		return w.fail()
	}

	// // For each agreement protocol in the current list of configured policies, startup a processor
	// // to initiate the protocol.
	// for protocolName, _ := range w.pm.GetAllAgreementProtocols() {
	// 	if policy.SupportedAgreementProtocol(protocolName) {
	// 		cph := CreateConsumerPH(protocolName, w.BaseWorker.Manager.Config, w.db, w.pm, w.BaseWorker.Manager.Messages, w.MMSObjectPM, w.matchCache)
	// 		cph.Initialize()
	// 		w.consumerPH[protocolName] = cph
	// 	} else {
	// 		glog.Errorf("AgreementBotWorker ignoring agreement protocol %v, not supported.", protocolName)
	// 	}
	// }

	// Sync up between what's in our database versus what's in the exchange. The governance routine will cancel any
	// agreements whose state might have changed
	// while the agbot was down. We will also check to make sure that policies havent changed. If they have, then
	// we will cancel agreements and allow them to re-negotiate.
	if err := w.syncOnInit(); err != nil {
		glog.Errorf("AgreementBotWorker Terminating, unable to sync up, error: %v", err)
		return w.fail()
	}

	// TODO: Do we need this?
	// The agbot worker is now ready to handle incoming messages
	w.ready = true

	// Start the governance routines using the subworker APIs.
	w.DispatchSubworker(GOVERN_AGREEMENTS, w.GovernAgreements, int(w.BaseWorker.Manager.Config.AgreementBot.ProcessGovernanceIntervalS), false)
	w.DispatchSubworker(GOVERN_ARCHIVED_AGREEMENTS, w.GovernArchivedAgreements, 1800, false)
	//w.DispatchSubworker(GOVERN_BC_NEEDS, w.GovernBlockchainNeeds, 60, false)
	w.DispatchSubworker(MESSAGE_KEY_CHECK, w.messageKeyCheck, w.BaseWorker.Manager.Config.AgreementBot.MessageKeyCheck, false)

	// if w.Config.AgreementBot.CheckUpdatedPolicyS != 0 {
	// 	// Use custom subworker APIs for the policy watcher because it is stateful and already does its own time management.
	// 	ch := w.AddSubworker(POLICY_WATCHER)
	// 	go w.policyWatcher(POLICY_WATCHER, ch)
	// }

	return true
}

func (w *AgreementBotWorker) CommandHandler(command worker.Command) bool {

	// Enter the command processing loop. Initialization is complete so wait for commands to
	// perform. Commands are created as the result of events that are triggered elsewhere
	// in the system. This function also wakes up periodically and looks for messages on
	// its exchange message queue.

	switch command.(type) {
	case *BlockchainEventCommand:
		cmd, _ := command.(*BlockchainEventCommand)
		// Put command on each protocol worker's command queue
		protocols := w.consumerPH.GetAll()
		for _, p := range protocols {
			if w.consumerPH.Get(p).AcceptCommand(cmd) {
				w.consumerPH.Get(p).HandleBlockchainEvent(cmd)
			}
		}

	// case *PolicyChangedCommand:
	// 	// This event indicates that a specific policy has actually changed, and action is needed.
	// 	// TODO: re-evaluate all of this
	// 	cmd := command.(*PolicyChangedCommand)

	// 	if pol, err := policy.DemarshalPolicy(cmd.Msg.PolicyString()); err != nil {
	// 		glog.Errorf(fmt.Sprintf("AgreementBotWorker error demarshalling change event policy %v, error: %v", cmd.Msg.PolicyString(), err))
	// 	} else {
	// 		// We know that all agreement protocols in the policy are supported by this runtime. If not, then this
	// 		// event would not have occurred.

	// 		// glog.V(5).Infof("AgreementBotWorker about to update policy in PM.")
	// 		// // Update the policy in the policy manager.
	// 		// w.pm.UpdatePolicy(cmd.Msg.Org(), pol)
	// 		// glog.V(5).Infof("AgreementBotWorker updated policy in PM.")

	// 		// for _, agp := range pol.AgreementProtocols {
	// 		// 	// Update the protocol handler map and make sure there are workers available if the policy has a new protocol in it.
	// 		// 	if !w.consumerPH.Has(agp.Name) {
	// 		// 		glog.V(3).Infof("AgreementBotWorker creating worker pool for new agreement protocol %v", agp.Name)
	// 		// 		cph := CreateConsumerPH(agp.Name, w.BaseWorker.Manager.Config, w.db, w.pm, w.BaseWorker.Manager.Messages, w.MMSObjectPM, w.matchCache)
	// 		// 		cph.Initialize()
	// 		// 		w.consumerPH.Add(agp.Name, cph)
	// 		// 	}
	// 		// }

	// 		// Send the policy change command to all protocol handlers just in case an agreement protocol was
	// 		// deleted from the new policy file.

	// 		// This is for evaluating existing agreements. This could generate 1000s of cancel requests. The
	// 		// Prioritized work queue buffer should be able to handle this.

	// 		protocols := w.consumerPH.GetAll()
	// 		for _, agp := range protocols {
	// 			// Queue the command to the relevant protocol handler for further processing.
	// 			if w.consumerPH.Get(agp).AcceptCommand(cmd) {
	// 				w.consumerPH.Get(agp).HandlePolicyChanged(cmd, w.consumerPH.Get(agp))
	// 			}
	// 		}

	// 	}

	// case *PolicyDeletedCommand:
	// 	cmd := command.(*PolicyDeletedCommand)

	// 	if pol, err := policy.DemarshalPolicy(cmd.Msg.PolicyString()); err != nil {
	// 		glog.Errorf(fmt.Sprintf("AgreementBotWorker error demarshalling change event policy %v, error: %v", cmd.Msg.PolicyString(), err))
	// 	} else {

	// 		glog.V(5).Infof("AgreementBotWorker about to delete policy from PM.")
	// 		// Update the policy in the policy manager.
	// 		w.pm.DeletePolicy(cmd.Msg.Org(), pol)
	// 		glog.V(5).Infof("AgreementBotWorker deleted policy from PM.")

	// 		// Queue the command to the correct protocol worker pool(s) for further processing. The deleted policy
	// 		// might not contain a supported protocol, so we need to check that first.
	// 		for _, agp := range pol.AgreementProtocols {
	// 			if w.consumerPH.Has(agp.Name) {
	// 				if w.consumerPH.Get(agp.Name).AcceptCommand(cmd) {
	// 					w.consumerPH.Get(agp.Name).HandlePolicyDeleted(cmd, w.consumerPH.Get(agp.Name))
	// 				}
	// 			} else {
	// 				glog.Infof("AgreementBotWorker ignoring policy deleted command for unsupported agreement protocol %v", agp.Name)
	// 			}
	// 		}
	// 	}

	// TODO: get rid of this, update BPM directly with lock.
	// case *CacheServicePolicyCommand:
	// 	cmd, _ := command.(*CacheServicePolicyCommand)
	// 	w.BusinessPolManager.AddMarshaledServicePolicy(cmd.Msg.BusinessPolOrg, cmd.Msg.BusinessPolName, cmd.Msg.ServiceId, cmd.Msg.ServicePolicy)

	// case *ServicePolicyChangedCommand:
	// 	cmd, _ := command.(*ServicePolicyChangedCommand)
	// 	// Send the service policy changed command to all protocol handlers
	// 	protocols := w.consumerPH.GetAll()
	// 	for _, agp := range protocols {
	// 		// Queue the command to the relevant protocol handler for further processing.
	// 		if w.consumerPH.Get(agp).AcceptCommand(cmd) {
	// 			w.consumerPH.Get(agp).HandleServicePolicyChanged(cmd, w.consumerPH.Get(agp))
	// 		}
	// 	}

	// case *ServicePolicyDeletedCommand:
	// 	cmd, _ := command.(*ServicePolicyDeletedCommand)
	// 	// Send the service policy deleted command to all protocol handlers
	// 	protocols := w.consumerPH.GetAll()
	// 	for _, agp := range protocols {
	// 		// Queue the command to the relevant protocol handler for further processing.
	// 		if w.consumerPH.Get(agp).AcceptCommand(cmd) {
	// 			w.consumerPH.Get(agp).HandleServicePolicyDeleted(cmd, w.consumerPH.Get(agp))
	// 		}
	// 	}

	case *AgreementTimeoutCommand:
		cmd, _ := command.(*AgreementTimeoutCommand)
		if !w.consumerPH.Has(cmd.Protocol) {
			glog.Errorf(fmt.Sprintf("AgreementBotWorker unable to process agreement timeout command %v due to unknown agreement protocol", cmd))
		} else {
			if w.consumerPH.Get(cmd.Protocol).AcceptCommand(cmd) {
				w.consumerPH.Get(cmd.Protocol).HandleAgreementTimeout(cmd, w.consumerPH.Get(cmd.Protocol))
			}
		}

	case *WorkloadUpgradeCommand:
		cmd, _ := command.(*WorkloadUpgradeCommand)
		// The workload upgrade request might not involve a specific agreement, so we can't know precisely which agreement
		// protocol might be relevant. Therefore we will send this upgrade to all protocol worker pools.
		protocols := w.consumerPH.GetAll()
		for _, ch := range protocols {
			if w.consumerPH.Get(ch).AcceptCommand(cmd) {
				w.consumerPH.Get(ch).HandleWorkloadUpgrade(cmd, w.consumerPH.Get(ch))
			}
		}

	case *AccountFundedCommand:
		cmd, _ := command.(*AccountFundedCommand)
		protocols := w.consumerPH.GetAll()
		for _, cph := range protocols {
			w.consumerPH.Get(cph).SetBlockchainWritable(&cmd.Msg)
		}

	case *ClientInitializedCommand:
		cmd, _ := command.(*ClientInitializedCommand)
		protocols := w.consumerPH.GetAll()
		for _, cph := range protocols {
			w.consumerPH.Get(cph).SetBlockchainClientAvailable(&cmd.Msg)
		}

	case *ClientStoppingCommand:
		cmd, _ := command.(*ClientStoppingCommand)
		protocols := w.consumerPH.GetAll()
		for _, cph := range protocols {
			w.consumerPH.Get(cph).SetBlockchainClientNotAvailable(&cmd.Msg)
		}

	case *MMSObjectPolicyEventCommand:
		cmd, _ := command.(*MMSObjectPolicyEventCommand)
		protocols := w.consumerPH.GetAll()
		for _, ch := range protocols {
			if w.consumerPH.Get(ch).AcceptCommand(cmd) {
				w.consumerPH.Get(ch).HandleMMSObjectPolicy(cmd, w.consumerPH.Get(ch))
			}
		}

	case *ObjectPoliciesChangeCommand:
		cmd, _ := command.(*ObjectPoliciesChangeCommand)
		go w.handleObjectPoliciesChange(&cmd.Msg)

	case *MessageCommand:
		w.processProtocolMessage()

	case *PatternChangeCommand:
		//cmd, _ := command.(*PatternChangeCommand)
		w.getAllPatterns()
		w.ensureProtocolWorkers()

	case *PolicyChangeCommand:
		// This command gets called when the exchange detects that a deployment policy changed.
		cmd, _ := command.(*PolicyChangeCommand)

		// Extract the details of the change from the event message.
		change := cmd.Msg.GetChange()
		polChange, ok := change.(exchange.ExchangeChange)
		if !ok {
			glog.Errorf(AWlogString(fmt.Sprintf("wrong type for exchange deployment policy change %v (%T)", change, change)))
		}

		// Query exchange for the changed or deleted deployment policy and update the cache.
		if polChange.IsDeploymentPolicy() {
			w.handleDeploymentPolicyChange(&polChange)
		}

	case *ServicePolicyChangeCommand:
		cmd, _ := command.(*ServicePolicyChangeCommand)

		// Extract the details of the change from the event message.
		change := cmd.Msg.GetChange()
		polChange, ok := change.(exchange.ExchangeChange)
		if !ok {
			glog.Errorf(AWlogString(fmt.Sprintf("wrong type for exchange service policy change %v (%T)", change, change)))
		}

		// Service policies are added to the policy manager dynamically, as the need for them is discovered by the
		// agreement workers. Therefore, updates to service policies might not be interesting to cache. Only
		// service policies that are already in the cache need to be updated.
		if polChange.IsServicePolicy() {
			w.handleServicePolicyChange(&polChange)
		}

	case *ServedPatternCommand:
		w.saveAgbotServedPatterns() // Save the served orgs and pattern names to the pattern cache.
		w.getAllPatterns()          // Read all served patterns into the pattern cache.
		w.ensureProtocolWorkers()   // Make sure there is a protocol worker pool for any new agreement protocols that might appear in the new patterns.
		w.lastSearchPatComplete = false
		go w.scanAllPatternsAndMakeAgreements() // Do a full scan of nodes across all patterns.

	case *ServedPolicyCommand:
		w.saveAgbotServedPolicies() // Save the served orgs and policy names to the deployment policy cache.
		w.getAllDeploymentPols()    // Read all served business policies into the deployment policy cache.
		w.ensureProtocolWorkers()   // Make sure there is a protocol worker pool for any new agreement protocols that might appear in the new policies.
		w.lastSearchPolComplete = false
		go w.scanAllPoliciesAndMakeAgreements() // Do a full scan of nodes across all policies.

	case *NodeChangeCommand:
		cmd, _ := command.(*NodeChangeCommand)
		w.collectChangedNode(&cmd.Msg)

	case *NodePolicyChangeCommand:
		cmd, _ := command.(*NodePolicyChangeCommand)
		change := cmd.Msg.GetChange()
		nodeChange, ok := change.(exchange.ExchangeChange)
		if !ok {
			glog.Errorf(AWlogString(fmt.Sprintf("wrong type for exchange change %v (%T)", change, change)))
		} else if nodeChange.IsNodePolicy("") {
			w.handleNodePolicyChange(nodeChange.GetFullID())
		}

	case *AgbotShutdownCommand:
		w.shutdownStarted = true
		glog.V(4).Infof("AgreementBotWorker received start shutdown command")

	default:
		return false
	}

	// TODO: call the NoWorkhandler if it's been a while, just in case. The default case above should be changed.

	return true

}

func (w *AgreementBotWorker) processProtocolMessage() {
	glog.V(3).Infof(fmt.Sprintf("AgreementBotWorker retrieving messages from the exchange"))

	if msgs, err := w.getMessages(); err != nil {
		glog.Errorf(fmt.Sprintf("AgreementBotWorker unable to retrieve exchange messages, error: %v", err))
	} else {
		// Loop through all the returned messages and process them.
		for _, msg := range msgs {

			glog.V(3).Infof(fmt.Sprintf("AgreementBotWorker reading message %v from the exchange", msg.MsgId))
			// First get my own keys
			_, myPrivKey, _ := exchange.GetKeys(w.Config.AgreementBot.MessageKeyPath)

			// Deconstruct and decrypt the message. If there is a problem with the message, it will be deleted.
			deleteMessage := true
			if protocolMessage, receivedPubKey, err := exchange.DeconstructExchangeMessage(msg.Message, myPrivKey); err != nil {
				glog.Errorf(fmt.Sprintf("AgreementBotWorker unable to deconstruct exchange message %v, error %v", msg, err))
			} else if serializedPubKey, err := exchange.MarshalPublicKey(receivedPubKey); err != nil {
				glog.Errorf(fmt.Sprintf("AgreementBotWorker unable to marshal the key from the encrypted message %v, error %v", receivedPubKey, err))
			} else if bytes.Compare(msg.DevicePubKey, serializedPubKey) != 0 {
				glog.Errorf(fmt.Sprintf("AgreementBotWorker sender public key from exchange %x is not the same as the sender public key in the encrypted message %x", msg.DevicePubKey, serializedPubKey))
			} else if msgProtocol, err := abstractprotocol.ExtractProtocol(string(protocolMessage)); err != nil {
				glog.Errorf(fmt.Sprintf("AgreementBotWorker unable to extract agreement protocol name from message %v", protocolMessage))
			} else if !w.consumerPH.Has(msgProtocol) {
				glog.Infof(fmt.Sprintf("AgreementBotWorker unable to direct exchange message %v to a protocol handler, deleting it.", protocolMessage))
				deleteMessage = false
				DeleteMessage(msg.MsgId, w.GetExchangeId(), w.GetExchangeToken(), w.GetExchangeURL(), w.httpClient)
			} else {
				// The message seems to be good, so don't delete it yet, the protocol worker that handles the message will delete it.
				deleteMessage = false

				// Send the message to a protocol worker.
				cmd := NewNewProtocolMessageCommand(protocolMessage, msg.MsgId, msg.DeviceId, msg.DevicePubKey)
				if !w.consumerPH.Get(msgProtocol).AcceptCommand(cmd) {
					glog.Infof(fmt.Sprintf("AgreementBotWorker protocol handler for %v not accepting exchange messages, deleting msg.", msgProtocol))
					DeleteMessage(msg.MsgId, w.GetExchangeId(), w.GetExchangeToken(), w.GetExchangeURL(), w.httpClient)
				} else if err := w.consumerPH.Get(msgProtocol).DispatchProtocolMessage(cmd, w.consumerPH.Get(msgProtocol)); err != nil {
					DeleteMessage(msg.MsgId, w.GetExchangeId(), w.GetExchangeToken(), w.GetExchangeURL(), w.httpClient)
				}

			}

			// If anything went wrong trying to decrypt the message or verify its origin, etc, just delete it. These errors aren't
			// expected to be retryable.
			if deleteMessage {
				DeleteMessage(msg.MsgId, w.GetExchangeId(), w.GetExchangeToken(), w.GetExchangeURL(), w.httpClient)
			}

		}
	}
	glog.V(3).Infof(fmt.Sprintf("AgreementBotWorker done processing messages"))
}

func (w *AgreementBotWorker) NoWorkHandler() {

	// Handle deferred commands.
	glog.V(3).Infof(AWlogString("queueing deferred commands"))
	protocols := w.consumerPH.GetAll()
	for _, cph := range protocols {
		w.consumerPH.Get(cph).HandleDeferredCommands()
	}
	glog.V(4).Infof(AWlogString("done queueing deferred commands"))


	glog.V(3).Infof(AWlogString(fmt.Sprintf("Match Cache: %v", w.matchCache)))


	// If shutdown has not started then keep looking for nodes to make agreements with. This can be a very long running and
	// expensive operation so it will be dispatched onto a separate go thread.
	// If shutdown has started then we will stop making new agreements. Instead we will look for agreements that have not yet completed
	// the agreement protocol process. If there are any, then we will hold the quiesce from completing.
	if !w.ShutdownStarted() {

		// If the previous searches have completed, remember them and log the completion.
		select {
		case w.lastSearchPolComplete = <-w.searchPolThread:
			glog.V(3).Infof(AWlogString("Done Polling Exchange for policies."))
		default:
			if !w.lastSearchPolComplete {
				glog.V(5).Infof(AWlogString("waiting for policy search results."))
			}
		}

		select {
		case w.lastSearchPatComplete = <-w.searchPatThread:
			glog.V(3).Infof(AWlogString("Done Polling Exchange for patterns."))
		default:
			if !w.lastSearchPatComplete {
				glog.V(5).Infof(AWlogString("waiting for pattern search results."))
			}
		}

		// If there are nodes to analyze, and a search has not been started recently, start one now
		// by kicking off a seperate thread to do the analysis.
		if len(w.changedNodes) != 0 && w.lastSearchPolComplete && w.lastSearchPatComplete && ((uint64(time.Now().Unix()) - w.lastSearchTime) >= uint64(w.Config.AgreementBot.NewContractIntervalS)) {

			// Sort the nodes into patterns and policies and make a map of each that the subthread can own. The changedNodes map cant be changing because it is only updated
			// by the main agbot thread.
			polNodes := make(map[string]*exchange.Device)
			patNodes := make(map[string]*exchange.Device)
			glog.V(3).Infof(AWlogString(fmt.Sprintf("processing collected nodes %v", w.changedNodes)))
			for nodeId, _ := range w.changedNodes {
				if node, err := exchange.GetHTTPDeviceHandler(w)(nodeId, ""); err != nil {
					glog.Errorf(AWlogString(fmt.Sprintf("error retrieving node %v, error: %v", nodeId, err)))
					// The node is deleted, remove agreements and clean up the match cache.
					w.handleNodePolicyChange(nodeId)
					w.matchCache.InvalidateNode(nodeId)
				} else if node.PublicKey == "" {
					glog.V(5).Infof(AWlogString(fmt.Sprintf("skipping node %v, node is not ready to exchange messages", nodeId)))
					continue
				} else if node.Pattern == "" {
					polNodes[nodeId] = node
				} else {
					patNodes[nodeId] = node
				}
			}

			// Now that there are pattern and policy node lists, kick off a subthread for each as necessary.
			if len(polNodes) != 0 || len(patNodes) != 0 {
				w.lastSearchTime = uint64(time.Now().Unix())
			}

			if len(polNodes) != 0 {
				glog.V(3).Infof(AWlogString("Polling Exchange for policies."))
				w.lastSearchPolComplete = false
				go w.evaluateMatchCacheForNodes(polNodes)
			}

			if len(patNodes) != 0 {
				glog.V(3).Infof(AWlogString("Polling Exchange for patterns."))
				w.lastSearchPatComplete = false
				go w.evaluateNodesForPatternAgreement(patNodes)
			}

			// Clear out the main thread's set of changed nodes so that it can begin collecting them again.
			w.changedNodes = make(map[string]interface{})

		}

		// There is a window where a deployment policy has been recently created or modified that MIGHT match these nodes,
		// but which hasn't been added to the match cache yet because the exchange search results are still working their
		// way through the protocol workers. To deal with this, the agbot will periodically initiate a full search
		// across all policies, for any nodes that might fall into this window.

		// TODO fix this






	} else {
		// Find all agreements that are not yet finalized. This filter will return only agreements that are still in an agreement protocol
		// pending state.
		glog.V(4).Infof("AgreementBotWorker Looking for pending agreements before shutting down.")

		agreementPendingFilter := func() persistence.AFilter {
			return func(a persistence.Agreement) bool { return a.AgreementFinalizedTime == 0 && a.AgreementTimedout == 0 }
		}

		// Look at all agreements across all protocols,
		foundPending := false
		for _, agp := range policy.AllAgreementProtocols() {

			// Find all agreements that are in progress, agreements that are not archived and dont have either a finalized time or a timeeout time.
			if agreements, err := w.db.FindAgreements([]persistence.AFilter{agreementPendingFilter(), persistence.UnarchivedAFilter()}, agp); err != nil {
				glog.Errorf("AgreementBotWorker unable to read agreements from database, error: %v", err)
				w.Messages() <- events.NewNodeShutdownCompleteMessage(events.AGBOT_QUIESCE_COMPLETE, err.Error())
			} else if len(agreements) != 0 {
				foundPending = true
				break
			}

		}

		// If no pending agreements were found, then we can begin the shutdown.
		if !foundPending {

			glog.V(5).Infof("AgreementBotWorker shutdown beginning")

			w.SetWorkerShuttingDown(0, 0)

			// Shutdown the protocol specific agreement workers for each supported protocol.
			protocols := w.consumerPH.GetAll()
			for _, cph := range protocols {
				w.consumerPH.Get(cph).HandleStopProtocol(w.consumerPH.Get(cph))
			}

			// Shutdown the subworkers.
			w.TerminateSubworkers()

			// Shutdown the database partition.
			w.db.QuiescePartition()

			w.Messages() <- events.NewNodeShutdownCompleteMessage(events.AGBOT_QUIESCE_COMPLETE, "")

		}
	}

}

func (w *AgreementBotWorker) collectChangedNode(ev *events.ExchangeChangeMessage) {
	change := ev.GetChange()
	nodeChange, ok := change.(exchange.ExchangeChange)
	if !ok {
		glog.Errorf(AWlogString(fmt.Sprintf("wrong type for exchange change %v (%T)", change, change)))
		return
	}

	if nodeChange.IsNode("") || nodeChange.IsNodeAgreement("") {
		nodeId := nodeChange.GetFullID()
		w.changedNodes[nodeId] = true
		glog.V(3).Infof(AWlogString(fmt.Sprintf("collected node %v", nodeId)))
	}
}

// // TODO: might be able to get rid of this.
// func (w *AgreementBotWorker) invalidateNodePolicyCache(ev *events.ExchangeChangeMessage) {
// 	change := ev.GetChange()
// 	nodePolicyChange, ok := change.(exchange.ExchangeChange)
// 	if !ok {
// 		glog.Errorf(AWlogString(fmt.Sprintf("wrong type for exchange change %v (%T)", change, change)))
// 		return
// 	}

// 	if nodePolicyChange.IsNodePolicy("") {
// 		nodeId := nodePolicyChange.GetFullID()
// 		w.matchCache.InvalidateNode(nodeId)
// 		glog.V(3).Infof(AWlogString(fmt.Sprintf("invalidated policy cache for node %v", nodeId)))
// 	}
// }

// Search all nodes for all patterns and make agreements if necessary. The function is called
// when the list of served patterns for this agbot changes. That's something that almost never
// happens.
func (w *AgreementBotWorker) scanAllPatternsAndMakeAgreements() {

	// TODO: How to account for node orgs in the served pattern config?

	// Get a list of all the pattern orgs this agbot is serving.
	patternOrgs := w.PatternManager.GetAllPatternOrgs()
	for _, org := range patternOrgs {

		// Get a copy of all patterns in the pattern manager so that we can safely iterate the list.
		patternNames := w.PatternManager.GetAllPatterns(org)
		for _, name := range patternNames {

			policies := w.PatternManager.GetPatternPolicies(org, name)
			for _, pol := range policies {
				w.searchNodesAndMakeAgreements(pol, org, "", 0)
			}
		}
	}

	w.searchPatThread <- true
}

// For a given set of pattern based nodes, search the internal policies related to the pattern
// to see if any of these nodes need agreements.
func (w *AgreementBotWorker) evaluateNodesForPatternAgreement(nodes map[string]*exchange.Device) {

	glog.V(3).Infof(AWlogString(fmt.Sprintf("evaluate pattern nodes %v", nodes)))
	// Given the provided nodes, analyze each one's pattern.
	for nodeId, node := range nodes {
		policies := w.PatternManager.GetPatternPolicies(exchange.GetOrg(node.Pattern), exchange.GetId(node.Pattern))
		if len(policies) == 0 {
			glog.Errorf(AWlogString(fmt.Sprintf("no internal policies found for pattern %v for node %v", node.Pattern, nodeId)))
			continue
		}

		// Iterate the pattern's policies and analyze each one for this node.
		for _, pol := range policies {
			dev := exchange.NewSearchResultDevice(nodeId, node.GetNodeType(), node.PublicKey)
			sendVerify := true
			w.dispatchNodeAnalysis(dev, pol, exchange.GetOrg(node.Pattern), pol.Header.Name, sendVerify)
		}
	}

	w.searchPatThread <- true
}

// The input nodes have changed recently so look for deployment policy matches to make agreements.
// The change could be:
// 1. New node registration
// 2. Updated node userinputs
// 3. Node deletion - Handled elsewhere? TODO
//
// This function is executed on its own thread and it reports back to the agbot NoWorkHandler
// when it's done.
func (w *AgreementBotWorker) evaluateMatchCacheForNodes(nodes map[string]*exchange.Device) {

	glog.V(3).Infof(AWlogString(fmt.Sprintf("evaluating match cache for nodes %v", nodes)))

	// Check the match cache to see if this node's policy is already known to match a set of
	// deployment policies.
	for nodeId, node := range nodes {
		nodePolicy, err := exchange.GetHTTPNodePolicyHandler(w)(nodeId)
		if err != nil {
			glog.Errorf(AWlogString(fmt.Sprintf("error retrieving node policy %v, error: %v", nodeId, err)))
			continue
		}

		ep := nodePolicy.GetExternalPolicy()
		// TODO: Deal with this hack
		hackedNP := ep.Properties.RemoveProperty(externalpolicy.PROP_NODE_HARDWAREID)
		ep.Properties = hackedNP

		if depPolicies, err := w.matchCache.GetCachedPolicies(&ep); err != nil {
			glog.Errorf(AWlogString(fmt.Sprintf("error matching node policy %v with match cache, error: %v", nodeId, err)))
		} else if len(depPolicies) == 0 {
			glog.V(3).Infof(AWlogString(fmt.Sprintf("No deployment policy matches for %v", nodeId)))
			//w.scanAllPoliciesForNode(nodeId, node)
			// TODO: Does this belong here?
		} else {
			glog.V(5).Infof(AWlogString(fmt.Sprintf("Found cached policy matches %v for %v", depPolicies, nodeId)))
			for depPolId, _ := range depPolicies {

				// Get a safe copy of the deployment policy.
				org, polName := cutil.SplitOrgSpecUrl(depPolId.AsString())
				bPol, _ := w.BusinessPolManager.GetBusinessPolicy(org, polName)
				if bPol == nil {
					glog.Errorf(AWlogString(fmt.Sprintf("deployment policy %v not found in deployment policy cache.", depPolId)))
				} else {
					dev := exchange.NewSearchResultDevice(nodeId, node.GetNodeType(), node.PublicKey)
					// The current node might already have agreements for some or all of these policies, so no need to send
					// a verify protocol msg if an agreement is found.
					sendVerify := false
					w.dispatchNodeAnalysis(dev, bPol, org, polName, sendVerify)
					//delete(nodes, nodeId)
				}
			}
		}
	}

	w.searchPolThread <- true
}

// This function is executed on sub threads
func (w *AgreementBotWorker) evaluateAllPoliciesForNode(nodeId string, dev *exchange.Device) {

	glog.V(3).Infof(AWlogString(fmt.Sprintf("evaluating all policies for node %v", nodeId)))

	// Get a list of the policy orgs that this agbot is serving.
	node := dev
	policyOrgs := w.BusinessPolManager.GetAllPolicyOrgs()
	for _, org := range policyOrgs {

		// Get a list of the deployment policy names this agbot is serving.
		policyNames := w.BusinessPolManager.GetAllBusinessPolicyNames(org)
		for _, bpName := range policyNames {
			_, polName := cutil.SplitOrgSpecUrl(bpName)

			// Get a safe copy of the deployment policy.
			bPol, _ := w.BusinessPolManager.GetBusinessPolicy(org, polName)
			if bPol == nil {
				glog.Errorf(AWlogString(fmt.Sprintf("deployment policy %v not found in deployment policy cache.", bpName)))
				continue
			}

			if node == nil {
				if exNode, err := exchange.GetHTTPDeviceHandler(w)(nodeId, ""); err != nil {
					glog.Errorf(AWlogString(fmt.Sprintf("error retrieving node %v, error: %v", nodeId, err)))
					// The node might be deleted.
					// TODO: handle this
				} else {
					node = exNode
				}
			}

			if node.PublicKey == "" {
				glog.V(5).Infof(AWlogString(fmt.Sprintf("for policy %v/%v, skipping node %v, node is not ready to exchange messages", org, polName, nodeId)))
				return
			} else {

				dev := exchange.NewSearchResultDevice(nodeId, node.GetNodeType(), node.PublicKey)
				// The current node might already have agreements for some or all of these policies, so no need to send
				// a verify protocol msg if an agreement is found.
				sendVerify := false
				w.dispatchNodeAnalysis(dev, bPol, org, polName, sendVerify)
			}
		}
	}
}

// Go through all the deployment polices and search for nodes.
// This function is executed on its own thread and it reports back to the agbot NoWorkHandler
// when it's done.
func (w *AgreementBotWorker) scanAllPoliciesAndMakeAgreements() {

	// TODO: How to account for node orgs in the served pattern config?

	// Current timestamp to be saved as the last agreement making cycle start time later.
	currentAgMakingStartTime := uint64(time.Now().Unix()) - 1

	// Get a list of the policy orgs that this agbot is serving.
	policyOrgs := w.BusinessPolManager.GetAllPolicyOrgs()
	for _, org := range policyOrgs {

		// Get a list of the deployment policy names this agbot is serving.
		policyNames := w.BusinessPolManager.GetAllBusinessPolicyNames(org)

		// Search the exchange for nodes that match the dpeloyment policy.
		for _, bpName := range policyNames {
			w.initiateDeploymentPolicySearch(org, bpName)
		}
	}

	// The current agreement making cycle is done, save the timestamp and tell the main thread that it's done.
	// The lastAgMakingTime is used by the exchange to filter out nodes that havent changed recently.
	w.lastAgMakingTime = currentAgMakingStartTime
	w.searchPolThread <- true
}

// Initiate a search for an input business policy.
func (w *AgreementBotWorker) initiateDeploymentPolicySearch(org string, depPolId string) {
	// Get a safe copy of the deployment policy, and last updated time.
	depPol, lastUpdated := w.BusinessPolManager.GetBusinessPolicy(org, depPolId)
	if depPol == nil {
		glog.Errorf(AWlogString(fmt.Sprintf("deployment policy %v not found in deployment policy cache.", depPolId)))
		return
	}

	// Search the exchange for nodes.
	_, polName := cutil.SplitOrgSpecUrl(depPolId)
	w.searchNodesAndMakeAgreements(depPol, org, polName, lastUpdated)
}

// Search the exchange and make agreements with any device that is eligible based on the policies we have and
// agreement protocols that we support.
func (w *AgreementBotWorker) searchNodesAndMakeAgreements(consumerPolicy *policy.Policy, org string, polName string, polLastUpdateTime uint64) {

	glog.V(3).Infof(AWlogString(fmt.Sprintf("searching %v", consumerPolicy.Header.Name)))
	if devices, err := w.searchExchange(consumerPolicy, org, polName, polLastUpdateTime); err != nil {
		glog.Errorf("AgreementBotWorker received error searching for %v, error: %v", consumerPolicy, err)
	} else {

		// For each node search result, dispatch a request to analyze the node against the input policy.
		// The exchange search API filters out nodes that already have an agreement for the deployment policy.
		// If a node is returned that already has an agreement, that could indicate a problem where the 
		// agent and agbot are out of sync on the agreement, so the abot will send an agreement verification.
		sendVerify := true
		for _, dev := range *devices {
			w.dispatchNodeAnalysis(&dev, consumerPolicy, org, polName, sendVerify)
		}
	}
}

func (w *AgreementBotWorker) dispatchNodeAnalysis(dev *exchange.SearchResultDevice, consumerPolicy *policy.Policy, org string, polName string, sendVerify bool) {

	glog.V(3).Infof("AgreementBotWorker picked up %v for policy %v.", dev.ShortString(), consumerPolicy.Header.Name)
	glog.V(5).Infof("AgreementBotWorker picked up %v", dev.String())

	// Check for agreements already in progress with this device
	if found, err := w.alreadyMakingAgreementWith(dev, consumerPolicy, sendVerify); err != nil {
		glog.Errorf("AgreementBotWorker received error trying to find pending agreements: %v", err)
		return
	} else if found {
		glog.V(5).Infof("AgreementBotWorker skipping device id %v, agreement attempt already in progress with %v", dev.Id, consumerPolicy.Header.Name)
		return
	}

	// If the device is not ready to make agreements yet, then skip it.
	if dev.PublicKey == "" {
		glog.V(5).Infof("AgreementBotWorker skipping device id %v, node is not ready to exchange messages", dev.Id)
		return
	}

	producerPolicy := policy.Policy_Factory(consumerPolicy.Header.Name)

	// Get the cached service policies from the business policy manager. The returned value
	// is a map keyed by the service id.
	// There could be many service versions defined in a businees policy.
	// The policy manager only caches the ones that are used by an old agreement for this business policy.
	// The cached ones may not be what the new agreement will use. If the new agreement chooses a
	// new service version, then the new service policy will be put into the cache.

	// TODO: defer service policy collection to the agbot worker

	svcPolicies := make(map[string]externalpolicy.ExternalPolicy, 0)
	if consumerPolicy.PatternId == "" {
		svcPolicies = w.BusinessPolManager.GetServicePoliciesForPolicy(org, polName)
	}

	// Select a worker pool based on the agreement protocol that will be used. This is decided by the
	// consumer policy.
	protocol := policy.Select_Protocol(producerPolicy, consumerPolicy)
	cmd := NewMakeAgreementCommand(*producerPolicy, *consumerPolicy, org, polName, *dev, svcPolicies)

	bcType, bcName, bcOrg := producerPolicy.RequiresKnownBC(protocol)

	if !w.consumerPH.Has(protocol) {
		glog.Errorf("AgreementBotWorker unable to find protocol handler for %v.", protocol)
	} else if bcType != "" && !w.consumerPH.Get(protocol).IsBlockchainWritable(bcType, bcName, bcOrg) {
		// Get that blockchain running if it isn't up.
		glog.V(5).Infof("AgreementBotWorker skipping device id %v, requires blockchain %v %v %v that isnt ready yet.", dev.Id, bcType, bcName, bcOrg)
		w.BaseWorker.Manager.Messages <- events.NewNewBCContainerMessage(events.NEW_BC_CLIENT, bcType, bcName, bcOrg, w.GetExchangeURL(), w.GetExchangeId(), w.GetExchangeToken())
		return
	} else if !w.consumerPH.Get(protocol).AcceptCommand(cmd) {
		glog.Errorf("AgreementBotWorker protocol handler for %v not accepting new agreement commands.", protocol)
	} else {
		w.consumerPH.Get(protocol).HandleMakeAgreement(cmd, w.consumerPH.Get(protocol))
		glog.V(5).Infof("AgreementBotWorker queued agreement attempt for policy %v and protocol %v", consumerPolicy.Header.Name, protocol)
	}
}

// Check all agreement protocol buckets to see if there are any agreements with this device.
func (w *AgreementBotWorker) alreadyMakingAgreementWith(dev *exchange.SearchResultDevice, consumerPolicy *policy.Policy, sendVerify bool) (bool, error) {

	// Check to see if there is already an agreement with this node.
	// This method is called when attempting to make a new agreement, but if there is already an agreement present
	// some agreement state might be out of sync. The verify msg will cause the systemto adjust if necessary.
	// The caller of this function knows if an existing agreement is expected or not.
	pendingAgreementFilter := func() persistence.AFilter {
		return func(a persistence.Agreement) bool {
			return a.DeviceId == dev.Id && a.PolicyName == consumerPolicy.Header.Name && a.AgreementTimedout == 0
		}
	}

	// Search all agreement protocol buckets
	for _, agp := range policy.AllAgreementProtocols() {
		// Find all agreements that are in progress. They might be waiting for a reply or not yet finalized.
		// TODO: To support more than 1 agreement (maxagreements > 1) with this device for this policy, we need to adjust this logic.
		if agreements, err := w.db.FindAgreements([]persistence.AFilter{persistence.UnarchivedAFilter(), pendingAgreementFilter()}, agp); err != nil {
			glog.Errorf(AWlogString(fmt.Sprintf("received error trying to find pending agreements for protocol %v: %v", agp, err)))
		} else if len(agreements) != 0 {

			ag := agreements[0]
			if ag.AgreementFinalizedTime != 0 && sendVerify {
				glog.V(5).Infof(AWlogString(fmt.Sprintf("sending agreement verify for %v", ag.CurrentAgreementId)))
				w.consumerPH.Get(ag.AgreementProtocol).VerifyAgreement(&ag, w.consumerPH.Get(ag.AgreementProtocol))
			}

			return true, nil
		}
	}
	return false, nil

}

// When a deployment policy changes (or is deleted), it can affect many existing agreements and could also
// cause new agreements to be made with existing nodes and didn't previously match.
//
// TODO: Consider that the deployment policy change was a change in the deployed service version or a change
// in the userinputs. The policy aspects might be compatible but the other aspects might require agreement
// cancellation.
//
// Called on main agbot thread.
func (w *AgreementBotWorker) handleDeploymentPolicyChange(polChange *exchange.ExchangeChange) {

	// Read the changed/deleted deployment policy from the exchange. If it's not there, the API call will return an
	// empty map.
	if exchPol, err := exchange.GetHTTPBusinessPoliciesHandler(w)(polChange.GetOrg(), polChange.GetID()); err != nil {
		glog.Errorf(AWlogString(fmt.Sprintf("unable to get deployment policy %v, error %v", polChange.GetFullID(), err)))
	} else if depPol, ok := exchPol[polChange.GetFullID()]; !ok {

		// Deployment policy is deleted. Remove it from the deployment policy cache and the match cache,
		// and then clean up any related agreements.
		depPol, _ := w.BusinessPolManager.GetBusinessPolicy(polChange.GetOrg(), polChange.GetID())
		if depPol != nil {

			w.BusinessPolManager.DeleteBusinessPolicy(polChange.GetOrg(), polChange.GetID())
			glog.V(3).Infof(AWlogString(fmt.Sprintf("deleted deployment policy %v from manager", polChange.GetFullID())))

			// Update the match cache to ensure that nodes which used to have matching policies are no longer
			// matched with the deleted policy.
			nl := w.matchCache.InvalidateDeploymentPolicy(polChange.GetFullID())
			glog.V(5).Infof(AWlogString(fmt.Sprintf("affected nodes %v", nl)))

			// Queue the command to the correct protocol worker pool(s) for further processing. The deleted policy
			// might not contain a supported protocol, so we need to check that first. This command will cancel
			// any agreements that the deleted policy was used to form. it could generate 1000s of requests to the
			// agreement worker pool(s).
			cmd := NewPolicyDeletedCommand(polChange.GetOrg(), depPol.Header.Name)
			for _, agp := range depPol.AgreementProtocols {
				if w.consumerPH.Has(agp.Name) {
					if w.consumerPH.Get(agp.Name).AcceptCommand(cmd) {
						w.consumerPH.Get(agp.Name).HandlePolicyDeleted(cmd, w.consumerPH.Get(agp.Name))
					}
				} else {
					glog.Infof(AWlogString(fmt.Sprintf("ignoring policy deleted command for unsupported agreement protocol %v", agp.Name)))
				}
			}
		}

	} else {
		// Update the policy in the cache if needed.
		bPol := depPol.GetBusinessPolicy()
		w.BusinessPolManager.UpdateBusinessPolicy(polChange.GetOrg(), polChange.GetFullID(), &bPol)
		w.ensureProtocolWorkers()
		glog.V(3).Infof(AWlogString(fmt.Sprintf("updated deployment policy %v in manager", polChange.GetFullID())))

		// Update the match cache to ensure that new nodes are evaluated correctly.
		nl := w.matchCache.InvalidateDeploymentPolicy(polChange.GetFullID())
		glog.V(5).Infof(AWlogString(fmt.Sprintf("affected nodes %v", nl)))

		// Evaluate existing agreements. This could generate 1000s of cancel requests to the worker threads.
		// TODO: We want to do this without scanning all the agreements, if possible.
		depPol, _ := w.BusinessPolManager.GetBusinessPolicy(polChange.GetOrg(), polChange.GetID())
		if depPol == nil {
			glog.Errorf(AWlogString(fmt.Sprintf("unable to get deployment policy %v from manager", polChange.GetFullID())))
		} else {
			cmd := NewPolicyChangedCommand(polChange.GetOrg(), depPol)
			protocols := w.consumerPH.GetAll()
			for _, agp := range protocols {
				// Queue the command to the relevant protocol handler for further processing.
				if w.consumerPH.Get(agp).AcceptCommand(cmd) {
					w.consumerPH.Get(agp).HandlePolicyChanged(cmd, w.consumerPH.Get(agp))
				}
			}
		}

		// Initiate a search with the updated policy. The search is being done on the main agbot thread under the
		// assumption that it's only a single search call, and therefore not overly expensive.
		w.initiateDeploymentPolicySearch(polChange.GetOrg(), polChange.GetID())
	}

}

// When a service policy changes (or is deleted), it can affect many existing agreements and could also
// cause new agreements to be made with existing nodes and didn't previously match.
func (w *AgreementBotWorker) handleServicePolicyChange(polChange *exchange.ExchangeChange) {

	// Read the changed/deleted service policy from the exchange. If it's not there, the API call will return an
	// empty map.
	// TODO: Validate the service policy
	if exchPol, err := exchange.GetHTTPServicePolicyWithIdHandler(w)(polChange.GetFullID()); err != nil {
		glog.Errorf(AWlogString(fmt.Sprintf("unable to get service policy %v, error %v", polChange.GetFullID(), err)))
	} else if exchPol == nil {

		// Service policy is deleted. Remove it from the deployment policy cache and the match cache,
		// and then clean up any related agreements.
		// depPol, _ := w.BusinessPolManager.GetBusinessPolicy(polChange.GetOrg(), polChange.GetID())
		// if depPol != nil {

		// 	w.BusinessPolManager.DeleteBusinessPolicy(polChange.GetOrg(), polChange.GetID())
		glog.V(3).Infof(AWlogString(fmt.Sprintf("deleted service policy %v from manager", polChange.GetFullID())))

		// 	// Update the match cache to ensure that nodes which used to have matching policies are no longer
		// 	// matched with the deleted policy.
		// 	nl := w.matchCache.InvalidateDeploymentPolicy(polChange.GetFullID())
		// 	glog.V(5).Infof(AWlogString(fmt.Sprintf("affected nodes %v", nl)))

		// 	// Queue the command to the correct protocol worker pool(s) for further processing. The deleted policy
		// 	// might not contain a supported protocol, so we need to check that first. This command will cancel
		// 	// any agreements that the deleted policy was used to form. it could generate 1000s of requests to the
		// 	// agreement worker pool(s).
		// 	cmd := NewPolicyDeletedCommand(polChange.GetOrg(), depPol.Header.Name)
		// 	for _, agp := range depPol.AgreementProtocols {
		// 		if w.consumerPH.Has(agp.Name) {
		// 			if w.consumerPH.Get(agp.Name).AcceptCommand(cmd) {
		// 				w.consumerPH.Get(agp.Name).HandlePolicyDeleted(cmd, w.consumerPH.Get(agp.Name))
		// 			}
		// 		} else {
		// 			glog.Infof(AWlogString(fmt.Sprintf("ignoring policy deleted command for unsupported agreement protocol %v", agp.Name)))
		// 		}
		// 	}
		// }

	} else {

		// Update the service policy in the cache. A list of deployment policies that are currently using this
		// service policy is returned. The UpdateServicePolicy function might return both an error and a list of deployment
		// policy ids.
		svcPol := exchPol.GetExternalPolicy()
		depPols, err := w.BusinessPolManager.UpdateServicePolicy(polChange.GetFullID(), &svcPol)
		if err != nil {
			glog.Errorf(AWlogString(fmt.Sprintf("unable to update service policy %v cache, error %v", polChange.GetFullID(), err)))
		}
		glog.V(3).Infof(AWlogString(fmt.Sprintf("updated service policy %v in manager, affected deployment policies %v", polChange.GetFullID(), depPols)))

		// If the changed service policy causes any deployment policies to become incompatible with known node
		// policies, then invalidate the match cache entries for those deployment policies.
		for _, depPolId := range depPols {
			depPolicy, _ := w.BusinessPolManager.GetBusinessPolicy(exchange.GetOrg(depPolId), exchange.GetId(depPolId))
			pPolicy, err := policy.MergePolicyWithExternalPolicy(depPolicy, &svcPol)
			if err != nil {
				glog.Errorf(AWlogString(fmt.Sprintf("unable to merge service policy %v and deployment policy %v, error %v", polChange.GetFullID(), depPolId, err)))
				continue
			}

			// The deployment and updated service policies are merged, now we need to compare them against the
			// node policies that used to be compatible.
			// Get the set of node policies from the match cache which are known to match the affected deployment policies.
			nps := w.matchCache.GetCompatibleNodePolicies(depPolId)

			for _, np := range nps {
				if err := policy.Are_Compatible(np, pPolicy, nil); err != nil {
					glog.Warningf(AWlogString(fmt.Sprintf("merged policy %v is incompatible with node policy %v, reason: %v", pPolicy.Header.Name, np, err)))
					// Update the match cache to ensure that new nodes are evaluated correctly. The list of nodes which now have
					// incompatible policy are returned.
					nl := w.matchCache.InvalidateDeploymentPolicy(depPolId)
					glog.V(5).Infof(AWlogString(fmt.Sprintf("affected nodes %v", nl)))

					// Send the service policy changed command to all protocol handlers to end the incompatible
					// agreements. This could generate 1000s of cancel requests to the worker threads.
					cmd := NewServicePolicyChangedCommand(exchange.GetOrg(depPolId), exchange.GetId(depPolId), nl)
					protocols := w.consumerPH.GetAll()
					for _, agp := range protocols {
						// Queue the command to the relevant protocol handler for further processing.
						if w.consumerPH.Get(agp).AcceptCommand(cmd) {
							w.consumerPH.Get(agp).HandleServicePolicyChanged(cmd, w.consumerPH.Get(agp))
						}
					}

				} else {
					glog.V(5).Infof(AWlogString(fmt.Sprintf("updated service policy %v still compatible with %v", polChange.GetFullID(), np)))
				}
			}

			// Initiate a search with the deployment policy. This search is done to pick up any nodes that are now compatible
			// with each of the relevant deployment policies (since it has effectively changed). The search is being done on the
			// main agbot thread under the assumption that it's only a small number of search calls, and therefore not overly expensive.
			w.initiateDeploymentPolicySearch(exchange.GetOrg(depPolId), exchange.GetId(depPolId))
		}
	}
}

// Determine if this change is compatible with the existing agreements.
func (w *AgreementBotWorker) handleNodePolicyChange(nodeId string) {

	glog.V(5).Infof(AWlogString(fmt.Sprintf("nodepolicy change %v detected", nodeId)))

	nodePolicy, err := exchange.GetHTTPNodePolicyHandler(w)(nodeId)
	if err != nil {
		glog.Errorf(AWlogString(fmt.Sprintf("error retrieving node policy %v, error: %v", nodeId, err)))
		return
	}

	ep := nodePolicy.GetExternalPolicy()
	// TODO: Deal with this hack
	hackedNP := ep.Properties.RemoveProperty(externalpolicy.PROP_NODE_HARDWAREID)
	ep.Properties = hackedNP

	if changed, err := w.matchCache.UpdateNodePolicy(nodeId, &ep); err != nil {
		glog.Errorf(AWlogString(fmt.Sprintf("error caching node policy for %v, error: %v", nodeId, err)))
	} else if changed {

		// Send the node changed command to all protocol handlers to end the incompatible
		// agreements. This could generate 1000s of cancel requests to the worker threads.
		policy, err := policy.GenPolicyFromExternalPolicy(&ep, nodeId)
		if err != nil {
			glog.Errorf(AWlogString(fmt.Sprintf("error retrieving node policy %v, error: %v", nodeId, err)))
			return
		}

		cmd := NewNodePolicyChangedCommand(nodeId, &ep, policy)
		protocols := w.consumerPH.GetAll()
		for _, agp := range protocols {
			// Queue the command to the relevant protocol handler for further processing.
			if w.consumerPH.Get(agp).AcceptCommand(cmd) {
				w.consumerPH.Get(agp).HandleNodePolicyChanged(cmd, w.consumerPH.Get(agp))
			}
		}

		// TODO: Batch these together?
		w.evaluateAllPoliciesForNode(nodeId, nil)
		glog.V(5).Infof(AWlogString(fmt.Sprintf("evaluated all policies for node %v", nodeId)))

	} else {
		glog.V(5).Infof(AWlogString(fmt.Sprintf("no change in node policy %v", nodeId)))
	}

}

// func (w *AgreementBotWorker) policyWatcher(name string, quit chan bool) {

// 	worker.GetWorkerStatusManager().SetSubworkerStatus(w.GetName(), name, worker.STATUS_STARTED)

// 	// create a place for the policy watcher to save state between iterations.
// 	contents := w.pm.WatcherContent

// 	for {
// 		glog.V(5).Infof(fmt.Sprintf("AgreementBotWorker checking for new or updated policy files"))
// 		select {
// 		case <-quit:
// 			w.Commands <- worker.NewSubWorkerTerminationCommand(name)
// 			glog.V(3).Infof(fmt.Sprintf("AgreementBotWorker %v exiting the subworker", name))
// 			return

// 		case <-time.After(time.Duration(w.Config.AgreementBot.CheckUpdatedPolicyS) * time.Second):
// 			contents, _ = policy.PolicyFileChangeWatcher(w.Config.AgreementBot.PolicyPath, contents, w.Config.ArchSynonyms, w.changedPolicy, w.deletedPolicy, w.errorPolicy, w.serviceResolver, 0)
// 		}
// 	}

// }

// // Functions called by the policy watcher
// func (w *AgreementBotWorker) changedPolicy(org string, fileName string, pol *policy.Policy) {
// 	glog.V(3).Infof(fmt.Sprintf("AgreementBotWorker detected changed policy file %v containing %v", fileName, pol))
// 	if policyString, err := policy.MarshalPolicy(pol); err != nil {
// 		glog.Errorf(fmt.Sprintf("AgreementBotWorker error trying to marshal policy %v error: %v", pol, err))
// 	} else {
// 		w.Messages() <- events.NewPolicyChangedMessage(events.CHANGED_POLICY, fileName, pol.Header.Name, org, policyString)
// 	}
// }

// func (w *AgreementBotWorker) deletedPolicy(org string, fileName string, pol *policy.Policy) {
// 	glog.V(3).Infof(fmt.Sprintf("AgreementBotWorker detected deleted policy file %v containing %v", fileName, pol))
// 	if policyString, err := policy.MarshalPolicy(pol); err != nil {
// 		glog.Errorf(fmt.Sprintf("AgreementBotWorker error trying to marshal policy %v error: %v", pol, err))
// 	} else {
// 		w.Messages() <- events.NewPolicyDeletedMessage(events.DELETED_POLICY, fileName, pol.Header.Name, org, policyString)
// 	}
// }

// func (w *AgreementBotWorker) errorPolicy(org string, fileName string, err error) {
// 	glog.Errorf(fmt.Sprintf("AgreementBotWorker tried to read policy file %v/%v, encountered error: %v", org, fileName, err))
// }

func (w *AgreementBotWorker) getMessages() ([]exchange.AgbotMessage, error) {
	var resp interface{}
	resp = new(exchange.GetAgbotMessageResponse)
	targetURL := w.GetExchangeURL() + "orgs/" + exchange.GetOrg(w.GetExchangeId()) + "/agbots/" + exchange.GetId(w.GetExchangeId()) + "/msgs"
	for {
		if err, tpErr := exchange.InvokeExchange(w.httpClient, "GET", targetURL, w.GetExchangeId(), w.GetExchangeToken(), nil, &resp); err != nil {
			glog.Errorf(err.Error())
			return nil, err
		} else if tpErr != nil {
			glog.Warningf(tpErr.Error())
			time.Sleep(10 * time.Second)
			continue
		} else {
			glog.V(3).Infof(fmt.Sprintf("AgreementBotWorker retrieved %v messages", len(resp.(*exchange.GetAgbotMessageResponse).Messages)))
			msgs := resp.(*exchange.GetAgbotMessageResponse).Messages
			return msgs, nil
		}
	}
}

// // This function runs through the agbot policy and builds a list of properties and values that
// // it wants to search on.
// func RetrieveAllProperties(version string, arch string, pol *policy.Policy) (*externalpolicy.PropertyList, error) {
// 	pl := new(externalpolicy.PropertyList)

// 	for _, p := range pol.Properties {
// 		*pl = append(*pl, p)
// 	}

// 	if version != "" {
// 		*pl = append(*pl, externalpolicy.Property{Name: "version", Value: version})
// 	}
// 	*pl = append(*pl, externalpolicy.Property{Name: "arch", Value: arch})

// 	if len(pol.AgreementProtocols) != 0 {
// 		*pl = append(*pl, externalpolicy.Property{Name: "agreementProtocols", Value: pol.AgreementProtocols.As_String_Array()})
// 	}

// 	return pl, nil
// }

func DeleteConsumerAgreement(httpClient *http.Client, url string, agbotId string, token string, agreementId string) error {

	glog.V(5).Infof(AWlogString(fmt.Sprintf("deleting agreement %v in exchange", agreementId)))

	var resp interface{}
	resp = new(exchange.PostDeviceResponse)
	targetURL := url + "orgs/" + exchange.GetOrg(agbotId) + "/agbots/" + exchange.GetId(agbotId) + "/agreements/" + agreementId
	for {
		if err, tpErr := exchange.InvokeExchange(httpClient, "DELETE", targetURL, agbotId, token, nil, &resp); err != nil && !strings.Contains(err.Error(), "not found") {
			glog.Errorf(AWlogString(fmt.Sprintf(err.Error())))
			return err
		} else if tpErr != nil {
			glog.Warningf(tpErr.Error())
			time.Sleep(10 * time.Second)
			continue
		} else {
			glog.V(5).Infof(AWlogString(fmt.Sprintf("deleted agreement %v from exchange", agreementId)))
			return nil
		}
	}

}

func DeleteMessage(msgId int, agbotId, agbotToken, exchangeURL string, httpClient *http.Client) error {
	var resp interface{}
	resp = new(exchange.PostDeviceResponse)
	targetURL := exchangeURL + "orgs/" + exchange.GetOrg(agbotId) + "/agbots/" + exchange.GetId(agbotId) + "/msgs/" + strconv.Itoa(msgId)
	for {
		if err, tpErr := exchange.InvokeExchange(httpClient, "DELETE", targetURL, agbotId, agbotToken, nil, &resp); err != nil {
			glog.Errorf(err.Error())
			return err
		} else if tpErr != nil {
			glog.Warningf(tpErr.Error())
			time.Sleep(10 * time.Second)
			continue
		} else {
			glog.V(3).Infof("Deleted exchange message %v", msgId)
			return nil
		}
	}
}

// Search the exchange for devices to make agreements with. The system should be operating such that devices are
// not returned from the exchange (for any given set of search criteria) once an agreement which includes those
// criteria has been reached. This prevents the agbot from continually sending proposals to devices that are
// already in an agreement.
//
// There are 2 ways to search the exchange; (a) by pattern and service or workload URL, or (b) by business policy.
// If the agbot is working with a policy file that was generated from a pattern, then it will do searches
// by pattern. If the agbot is working with a business policy, then it will do searches by the business policy.
func (w *AgreementBotWorker) searchExchange(pol *policy.Policy, polOrg string, polName string, polLastUpdateTime uint64) (*[]exchange.SearchResultDevice, error) {

	// If it is a pattern based policy, search by workload URL and pattern.
	if pol.PatternId != "" {
		// Get a list of node orgs that the agbot is serving for this pattern.
		nodeOrgs := w.PatternManager.GetServedNodeOrgs(polOrg, exchange.GetId(pol.PatternId))
		if len(nodeOrgs) == 0 {
			glog.V(3).Infof(AWlogString(fmt.Sprintf("Policy file for pattern %v exists but currently the agbot is not serving this policy for any organizations.", pol.PatternId)))
			empty := make([]exchange.SearchResultDevice, 0, 0)
			return &empty, nil
		}

		// Setup the search request body
		ser := exchange.CreateSearchPatternRequest()
		ser.SecondsStale = w.Config.AgreementBot.ActiveDeviceTimeoutS
		ser.NodeOrgIds = nodeOrgs
		ser.ServiceURL = cutil.FormOrgSpecUrl(pol.Workloads[0].WorkloadURL, pol.Workloads[0].Org)

		// Invoke the exchange
		var resp interface{}
		resp = new(exchange.SearchExchangePatternResponse)
		targetURL := w.GetExchangeURL() + "orgs/" + polOrg + "/patterns/" + exchange.GetId(pol.PatternId) + "/search"
		for {
			if err, tpErr := exchange.InvokeExchange(w.httpClient, "POST", targetURL, w.GetExchangeId(), w.GetExchangeToken(), ser, &resp); err != nil {
				if !strings.Contains(err.Error(), "status: 404") {
					return nil, err
				} else {
					empty := make([]exchange.SearchResultDevice, 0, 0)
					glog.V(3).Infof(AWlogString(fmt.Sprintf("search for %v/%v returned no nodes.", polOrg, pol.Header.Name)))
					return &empty, nil
				}
			} else if tpErr != nil {
				glog.Warningf(tpErr.Error())
				time.Sleep(10 * time.Second)
				continue
			} else {
				glog.V(3).Infof(AWlogString(fmt.Sprintf("found %v devices in exchange.", len(resp.(*exchange.SearchExchangePatternResponse).Devices))))
				dev := resp.(*exchange.SearchExchangePatternResponse).Devices
				return &dev, nil
			}
		}

	} else {
		// Get a list of node orgs that the agbot is serving for this business policy.
		nodeOrgs := w.BusinessPolManager.GetServedNodeOrgs(polOrg, polName)
		if len(nodeOrgs) == 0 {
			glog.V(3).Infof(AWlogString(fmt.Sprintf("Business policy %v/%v exists but currently the agbot is not serving this policy for any organizations.", polOrg, polName)))
			empty := make([]exchange.SearchResultDevice, 0, 0)
			return &empty, nil
		}

		// To make the search more efficient, the exchange only searchs the nodes what have been changed since bp_check_time.
		// If there is change for the business policy, all nodes need to be checked again.
		bp_check_time := w.lastAgMakingTime
		if polLastUpdateTime > w.lastAgMakingTime {
			bp_check_time = 0
		}

		// Setup the search request body
		ser := exchange.SearchExchBusinessPolRequest{
			NodeOrgIds:   nodeOrgs,
			ChangedSince: bp_check_time,
		}

		// Invoke the exchange
		var resp interface{}
		resp = new(exchange.SearchExchBusinessPolResponse)
		targetURL := w.GetExchangeURL() + "orgs/" + polOrg + "/business/policies/" + polName + "/search"
		for {
			if err, tpErr := exchange.InvokeExchange(w.httpClient, "POST", targetURL, w.GetExchangeId(), w.GetExchangeToken(), ser, &resp); err != nil {
				if !strings.Contains(err.Error(), "status: 404") {
					return nil, err
				} else {
					empty := make([]exchange.SearchResultDevice, 0, 0)
					glog.V(3).Infof(AWlogString(fmt.Sprintf("search for %v/%v returned no nodes.", polOrg, polName)))
					return &empty, nil
				}
			} else if tpErr != nil {
				glog.Warningf(tpErr.Error())
				time.Sleep(10 * time.Second)
				continue
			} else {
				glog.V(3).Infof(AWlogString(fmt.Sprintf("found %v devices in exchange.", len(resp.(*exchange.SearchExchBusinessPolResponse).Devices))))
				dev := resp.(*exchange.SearchExchBusinessPolResponse).Devices
				return &dev, nil
			}
		}
	}
}

// TODO: How to rebuild match cache after restart?
// TODO: How to rebuild after picking up a new partition?
func (w *AgreementBotWorker) syncOnInit() error {
	glog.V(3).Infof(AWlogString("beginning sync up."))

	// Search all agreement protocol buckets
	for _, agp := range policy.AllAgreementProtocols() {

		// Loop through our database and check each record for accuracy with the exchange and the blockchain
		if agreements, err := w.db.FindAgreements([]persistence.AFilter{persistence.UnarchivedAFilter()}, agp); err == nil {

			neededBCInstances := make(map[string]map[string]map[string]bool)

			for _, ag := range agreements {

				// Make a list of all blockchain instances in use by agreements in our DB so that we can make sure there is a
				// blockchain client running for each instance.
				bcType, bcName, bcOrg := w.consumerPH.Get(ag.AgreementProtocol).GetKnownBlockchain(&ag)

				if len(neededBCInstances[bcOrg]) == 0 {
					neededBCInstances[bcOrg] = make(map[string]map[string]bool)
				}
				if len(neededBCInstances[bcOrg][bcType]) == 0 {
					neededBCInstances[bcOrg][bcType] = make(map[string]bool)
				}
				neededBCInstances[bcOrg][bcType][bcName] = true

				// Check for any policy changes that might have occurred (policy deleted or changed) while the agbot was down.
				// TODO: Do we still need this?
				if ag.AgreementCreationTime != 0 {
					if pol, err := policy.DemarshalPolicy(ag.Policy); err != nil {
						glog.Errorf(AWlogString(fmt.Sprintf("unable to demarshal policy for agreement %v, error %v", ag.CurrentAgreementId, err)))
					} else if existingPol := w.pm.GetPolicy(ag.Org, pol.Header.Name); existingPol == nil {
						glog.Errorf(AWlogString(fmt.Sprintf("agreement %v has a policy %v that doesn't exist anymore", ag.CurrentAgreementId, pol.Header.Name)))
						// Update state in exchange
						if err := DeleteConsumerAgreement(w.Config.Collaborators.HTTPClientFactory.NewHTTPClient(nil), w.GetExchangeURL(), w.GetExchangeId(), w.GetExchangeToken(), ag.CurrentAgreementId); err != nil {
							glog.Errorf(AWlogString(fmt.Sprintf("error deleting agreement %v in exchange: %v", ag.CurrentAgreementId, err)))
						}
						// Remove any workload usage records so that a new agreement will be made starting from the highest priority workload
						if err := w.db.DeleteWorkloadUsage(ag.DeviceId, ag.PolicyName); err != nil {
							glog.Warningf(AWlogString(fmt.Sprintf("error deleting workload usage for %v using policy %v, error: %v", ag.DeviceId, ag.PolicyName, err)))
						}
						// Indicate that the agreement is timed out
						if _, err := w.db.AgreementTimedout(ag.CurrentAgreementId, agp); err != nil {
							glog.Errorf(AWlogString(fmt.Sprintf("error marking agreement %v terminated: %v", ag.CurrentAgreementId, err)))
						}
						w.consumerPH.Get(agp).HandleAgreementTimeout(NewAgreementTimeoutCommand(ag.CurrentAgreementId, ag.AgreementProtocol, w.consumerPH.Get(agp).GetTerminationCode(TERM_REASON_POLICY_CHANGED)), w.consumerPH.Get(agp))
					} else if err := w.pm.MatchesMine(ag.Org, pol); err != nil {
						glog.Warningf(AWlogString(fmt.Sprintf("agreement %v has a policy %v that has changed: %v", ag.CurrentAgreementId, pol.Header.Name, err)))

						// Remove any workload usage records (non-HA) or mark for pending upgrade (HA). There might not be a workload usage record
						// if the consumer policy does not specify the workload priority section.
						if wlUsage, err := w.db.FindSingleWorkloadUsageByDeviceAndPolicyName(ag.DeviceId, ag.PolicyName); err != nil {
							glog.Warningf(AWlogString(fmt.Sprintf("error retreiving workload usage for %v using policy %v, error: %v", ag.DeviceId, ag.PolicyName, err)))
						} else if wlUsage != nil && len(wlUsage.HAPartners) != 0 && wlUsage.PendingUpgradeTime != 0 {
							// Skip this agreement, it is part of an HA group where another member is upgrading
							continue
						} else if wlUsage != nil && len(wlUsage.HAPartners) != 0 && wlUsage.PendingUpgradeTime == 0 {
							for _, partnerId := range wlUsage.HAPartners {
								if _, err := w.db.UpdatePendingUpgrade(partnerId, ag.PolicyName); err != nil {
									glog.Warningf(AWlogString(fmt.Sprintf("could not update pending workload upgrade for %v using policy %v, error: %v", partnerId, ag.PolicyName, err)))
								}
							}
							// Choose this device's agreement within the HA group to start upgrading
							w.cleanupAgreement(&ag)
						} else {
							// Non-HA device or agrement without workload priority in the policy, re-make the agreement
							w.cleanupAgreement(&ag)
						}
						// } else if err := w.pm.AttemptingAgreement([]policy.Policy{*existingPol}, ag.CurrentAgreementId, ag.Org); err != nil {
						// 	glog.Errorf(AWlogString(fmt.Sprintf("cannot update agreement count for %v, error: %v", ag.CurrentAgreementId, err)))
						// } else if err := w.pm.FinalAgreement([]policy.Policy{*existingPol}, ag.CurrentAgreementId, ag.Org); err != nil {
						// 	glog.Errorf(AWlogString(fmt.Sprintf("cannot update agreement count for %v, error: %v", ag.CurrentAgreementId, err)))

						// There is a small window where an agreement might not have been recorded in the exchange. Let's just make sure.
					} else {

						var exchangeAgreement map[string]exchange.AgbotAgreement
						var resp interface{}
						resp = new(exchange.AllAgbotAgreementsResponse)
						targetURL := w.GetExchangeURL() + "orgs/" + exchange.GetOrg(w.GetExchangeId()) + "/agbots/" + exchange.GetId(w.GetExchangeId()) + "/agreements/" + ag.CurrentAgreementId

						if err, tpErr := exchange.InvokeExchange(w.httpClient, "GET", targetURL, w.GetExchangeId(), w.GetExchangeToken(), nil, &resp); err != nil || tpErr != nil {
							glog.Errorf(AWlogString(fmt.Sprintf("encountered error getting agbot info from exchange, error %v, transport error %v", err, tpErr)))
							continue
						} else {
							exchangeAgreement = resp.(*exchange.AllAgbotAgreementsResponse).Agreements
							glog.V(5).Infof(AWlogString(fmt.Sprintf("found agreements %v in the exchange.", exchangeAgreement)))

							if _, there := exchangeAgreement[ag.CurrentAgreementId]; !there {
								glog.V(3).Infof(AWlogString(fmt.Sprintf("agreement %v missing from exchange, adding it back in.", ag.CurrentAgreementId)))
								state := ""
								if ag.AgreementFinalizedTime != 0 {
									state = "Finalized Agreement"
								} else if ag.CounterPartyAddress != "" {
									state = "Producer Agreed"
								} else if ag.AgreementCreationTime != 0 {
									state = "Formed Proposal"
								} else {
									state = "unknown"
								}
								if err := w.recordConsumerAgreementState(ag.CurrentAgreementId, pol, ag.Org, state); err != nil {
									glog.Errorf(AWlogString(fmt.Sprintf("unable to record agreement %v state %v, error %v", ag.CurrentAgreementId, state, err)))
								}
							}
						}
						glog.V(3).Infof(AWlogString(fmt.Sprintf("agreement %v reconciled.", ag.CurrentAgreementId)))
					}

					// This state should never occur, but could if there was an error along the way. It means that a DB record
					// was created for this agreement but the record was never updated with the creation time, which is supposed to occur
					// immediately following creation of the record. Further, if this were to occur, then the exchange should not have been
					// updated, so there is no reason to try to clean that up. Same is true for the workload usage records.
				} else if ag.AgreementInceptionTime != 0 && ag.AgreementCreationTime == 0 {
					if err := w.db.DeleteAgreement(ag.CurrentAgreementId, agp); err != nil {
						glog.Errorf(AWlogString(fmt.Sprintf("error deleting partially created agreement: %v, error: %v", ag.CurrentAgreementId, err)))
					}
				}

			}

			// Fire off start requests for each BC client that we need running. The blockchain worker and the container worker will tolerate
			// a start request for containers that are already running.
			glog.V(3).Infof(AWlogString(fmt.Sprintf("discovered BC instances in DB %v", neededBCInstances)))
			for org, typeMap := range neededBCInstances {
				for typeName, instMap := range typeMap {
					for instName, _ := range instMap {
						w.Messages() <- events.NewNewBCContainerMessage(events.NEW_BC_CLIENT, typeName, instName, org, w.GetExchangeURL(), w.GetExchangeId(), w.GetExchangeToken())
					}
				}
			}

		} else {
			return errors.New(AWlogString(fmt.Sprintf("error searching database: %v", err)))
		}
	}

	glog.V(3).Infof(AWlogString("sync up completed normally."))
	return nil
}

func (w *AgreementBotWorker) cleanupAgreement(ag *persistence.Agreement) {
	// Update state in exchange
	if err := DeleteConsumerAgreement(w.Config.Collaborators.HTTPClientFactory.NewHTTPClient(nil), w.GetExchangeURL(), w.GetExchangeId(), w.GetExchangeToken(), ag.CurrentAgreementId); err != nil {
		glog.Errorf(AWlogString(fmt.Sprintf("error deleting agreement %v in exchange: %v", ag.CurrentAgreementId, err)))
	}

	// Delete this workload usage record so that a new agreement will be made starting from the highest priority workload
	if err := w.db.DeleteWorkloadUsage(ag.DeviceId, ag.PolicyName); err != nil {
		glog.Warningf(AWlogString(fmt.Sprintf("error deleting workload usage for %v using policy %v, error: %v", ag.DeviceId, ag.PolicyName, err)))
	}

	// Indicate that the agreement is timed out
	if _, err := w.db.AgreementTimedout(ag.CurrentAgreementId, ag.AgreementProtocol); err != nil {
		glog.Errorf(AWlogString(fmt.Sprintf("error marking agreement %v terminated: %v", ag.CurrentAgreementId, err)))
	}

	w.consumerPH.Get(ag.AgreementProtocol).HandleAgreementTimeout(NewAgreementTimeoutCommand(ag.CurrentAgreementId, ag.AgreementProtocol, w.consumerPH.Get(ag.AgreementProtocol).GetTerminationCode(TERM_REASON_POLICY_CHANGED)), w.consumerPH.Get(ag.AgreementProtocol))
}

func (w *AgreementBotWorker) recordConsumerAgreementState(agreementId string, pol *policy.Policy, org string, state string) error {

	workload := pol.Workloads[0].WorkloadURL

	glog.V(5).Infof(AWlogString(fmt.Sprintf("setting agreement %v for workload %v state to %v", agreementId, workload, state)))

	as := new(exchange.PutAgbotAgreementState)
	as.Service = exchange.WorkloadAgreement{
		Org:     exchange.GetOrg(pol.PatternId),
		Pattern: exchange.GetId(pol.PatternId),
		URL:     workload,
	}
	as.State = state

	var resp interface{}
	resp = new(exchange.PostDeviceResponse)
	targetURL := w.GetExchangeURL() + "orgs/" + exchange.GetOrg(w.GetExchangeId()) + "/agbots/" + exchange.GetId(w.GetExchangeId()) + "/agreements/" + agreementId
	for {
		if err, tpErr := exchange.InvokeExchange(w.httpClient, "PUT", targetURL, w.GetExchangeId(), w.GetExchangeToken(), &as, &resp); err != nil {
			glog.Errorf(err.Error())
			return err
		} else if tpErr != nil {
			glog.Warningf(tpErr.Error())
			time.Sleep(10 * time.Second)
			continue
		} else {
			glog.V(5).Infof(AWlogString(fmt.Sprintf("set agreement %v to state %v", agreementId, state)))
			return nil
		}
	}

}

func listContains(list string, target string) bool {
	ignoreAttribs := strings.Split(list, ",")
	for _, propName := range ignoreAttribs {
		if propName == target {
			return true
		}
	}
	return false
}

func (w *AgreementBotWorker) registerPublicKey() error {
	glog.V(5).Infof(AWlogString(fmt.Sprintf("registering agbot public key")))

	as := exchange.CreateAgbotPublicKeyPatch(w.Config.AgreementBot.MessageKeyPath)
	var resp interface{}
	resp = new(exchange.PostDeviceResponse)
	targetURL := w.GetExchangeURL() + "orgs/" + exchange.GetOrg(w.GetExchangeId()) + "/agbots/" + exchange.GetId(w.GetExchangeId())
	for {
		if err, tpErr := exchange.InvokeExchange(w.httpClient, "PATCH", targetURL, w.GetExchangeId(), w.GetExchangeToken(), &as, &resp); err != nil {
			glog.Errorf(err.Error())
			return err
		} else if tpErr != nil {
			glog.Warningf(tpErr.Error())
			time.Sleep(10 * time.Second)
			continue
		} else {
			glog.V(5).Infof(AWlogString(fmt.Sprintf("patched agbot public key %x", as)))
			return nil
		}
	}
}

func (w *AgreementBotWorker) serviceResolver(wURL string, wOrg string, wVersion string, wArch string) (*policy.APISpecList, error) {

	asl, _, _, err := exchange.GetHTTPServiceResolverHandler(w)(wURL, wOrg, wVersion, wArch)
	if err != nil {
		glog.Errorf(AWlogString(fmt.Sprintf("unable to resolve %v %v, error %v", wURL, wOrg, err)))
	}
	return asl, err
}

// Get the configured org/pattern/nodeorg triplet for this agbot.
func (w *AgreementBotWorker) saveAgbotServedPatterns() {
	servedPatterns, err := exchange.GetHTTPAgbotServedPattern(w)()
	if err != nil {
		glog.Errorf(AWlogString(fmt.Sprintf("unable to retrieve agbot served patterns, error %v", err)))
	}

	// Consume the configured org/pattern pairs into the PatternManager
	if err = w.PatternManager.SetCurrentPatterns(servedPatterns); err != nil {
		glog.Errorf(AWlogString(fmt.Sprintf("unable to process agbot served patterns %v, error %v", servedPatterns, err)))
	}
}

// Get the configured (policy org, business policy, node org) triplets for this agbot.
func (w *AgreementBotWorker) saveAgbotServedPolicies() {
	servedPolicies, err := exchange.GetHTTPAgbotServedDeploymentPolicy(w)()
	if err != nil {
		glog.Errorf(AWlogString(fmt.Sprintf("unable to retrieve agbot served deployment policies, error %v", err)))
	}

	// Consume the configured (policy org, business policy, node org) triplets into the BusinessPolicyManager
	if err = w.BusinessPolManager.SetCurrentBusinessPolicies(servedPolicies); err != nil {
		glog.Errorf(AWlogString(fmt.Sprintf("unable to process agbot served deployment policies %v, error %v", servedPolicies, err)))
	}

	// Consume the configured (policy org, business policy, node org) triplets into the ObjectPolicyManager
	if err = w.MMSObjectPM.SetCurrentPolicyOrgs(servedPolicies); err != nil {
		glog.Errorf(AWlogString(fmt.Sprintf("unable to process agbot served deployment policies for MMS %v, error %v", servedPolicies, err)))
	}

}

func (w *AgreementBotWorker) ensureProtocolWorkers() {
	// For each agreement protocol in the current list of configured policies, startup a processor
	// to initiate the protocol.
	for protocolName, _ := range w.pm.GetAllAgreementProtocols() {
		if policy.SupportedAgreementProtocol(protocolName) && !w.consumerPH.Has(protocolName) {
			cph := CreateConsumerPH(protocolName, w.BaseWorker.Manager.Config, w.db, w.pm, w.BaseWorker.Manager.Messages, w.MMSObjectPM, w.matchCache, w.BusinessPolManager)
			cph.Initialize()
			w.consumerPH.Add(protocolName, cph)
		}
	}
}

// A list of orgs and patterns is configured for the agbot to serve. Based on that config, retrieve all
// the patterns in the org and convert them to internal policies.
func (w *AgreementBotWorker) getAllPatterns() error {

	glog.V(5).Infof(AWlogString(fmt.Sprintf("retrieving patterns")))

	// Iterate over each org in the PatternManager and process all the patterns in that org
	for org, _ := range w.PatternManager.OrgPatterns {

		var exchangePatternMetadata map[string]exchange.Pattern
		var err error

		// check if the org exists on the exchange or not
		if _, err = exchange.GetOrganization(w.Config.Collaborators.HTTPClientFactory, org, w.GetExchangeURL(), w.GetExchangeId(), w.GetExchangeToken()); err != nil {
			// org does not exist is returned as an error
			glog.V(5).Infof(AWlogString(fmt.Sprintf("unable to get organization %v: %v", org, err)))
			exchangePatternMetadata = make(map[string]exchange.Pattern)
		} else {
			// Query exchange for all patterns in the org
			if exchangePatternMetadata, err = exchange.GetPatterns(w.Config.Collaborators.HTTPClientFactory, org, "", w.GetExchangeURL(), w.GetExchangeId(), w.GetExchangeToken()); err != nil {
				return errors.New(fmt.Sprintf("unable to get patterns for org %v, error %v", org, err))
			}
		}

		// Check for pattern metadata changes
		if err := w.PatternManager.UpdatePatternPolicies(org, exchangePatternMetadata); err != nil {
			return errors.New(fmt.Sprintf("unable to update policies for org %v, error %v", org, err))
		}
	}

	glog.V(5).Infof(AWlogString(fmt.Sprintf("done retrieving patterns")))
	return nil

}

// Read all the deployment policies from the exchange and cache them in the business policy manager.
func (w *AgreementBotWorker) getAllDeploymentPols() error {

	glog.V(5).Infof(AWlogString(fmt.Sprintf("retrieving deployment policies")))

	// Iterate over each org in the BusinessPolManager and retrieve the policies in that org.
	for _, org := range w.BusinessPolManager.GetAllPolicyOrgs() {

		exchPolsMetadata := make(map[string]exchange.ExchangeBusinessPolicy)
		var err error

		// Check if the org exists on the exchange. If the org is gone, an empty set of policies is
		// passed into the manager.
		if _, err = exchange.GetHTTPExchangeOrgHandler(w)(org); err != nil {
			glog.V(5).Infof(AWlogString(fmt.Sprintf("unable to get organization %v: %v", org, err)))
		} else {
			// Query exchange for all business policies in the org.
			if exchPolsMetadata, err = exchange.GetHTTPBusinessPoliciesHandler(w)(org, ""); err != nil {
				return errors.New(fmt.Sprintf("unable to get deployment polices for org %v, error %v", org, err))
			}
		}

		// Pass the deployment policies to the manager.
		if err := w.BusinessPolManager.UpdatePolicies(org, exchPolsMetadata); err != nil {
			return errors.New(fmt.Sprintf("unable to update deployment policies for org %v, error %v", org, err))
		}

	}

	glog.V(5).Infof(AWlogString(fmt.Sprintf("done retrieving deployment policies")))
	return nil

}

// The changes worker has produced a set of object changes that need to be processed.
func (w *AgreementBotWorker) handleObjectPoliciesChange(msg *events.MMSObjectPoliciesMessage) {

	glog.V(5).Infof(AWlogString(fmt.Sprintf("scanning object policies for updates")))

	// Extract the object policy changes from the event message and figure out which org the changes belong to
	// by looking at the first item in the list.
	objPolChanges, ok := msg.Policies.(exchange.ObjectDestinationPolicies)
	if !ok {
		glog.Errorf(AWlogString(fmt.Sprintf("unable to process object policy updates, type (%T) expected ObjectDestinationPolicies: %v", msg.Policies, msg.Policies)))
	} else if len(objPolChanges) == 0 {
		glog.Errorf(AWlogString(fmt.Sprintf("empty object destination policy changes")))
	}
	org := objPolChanges[0].OrgID

	// Check for policy metadata changes and update policies accordingly. Publish any status change events.
	if events, err := w.MMSObjectPM.UpdatePolicies(org, &objPolChanges, exchange.GetHTTPObjectQueryHandler(w)); err != nil {
		glog.Errorf(AWlogString(fmt.Sprintf("unable to update object policies for org %v, error %v", org, err)))
	} else {
		for _, ev := range events {
			w.Messages() <- ev
		}
	}

	glog.V(5).Infof(AWlogString(fmt.Sprintf("done scanning object policies for updates")))

}

// // For each business policy in the BusinessPolicyManager, this function updates the service policies with
// // the latest changes.
// func (w *AgreementBotWorker) updateServicePolicies(msg *events.ExchangeChangeMessage) {
// 	// map keyed by the service keys
// 	updatedServicePols := make(map[string]int, 10)

// 	glog.V(5).Infof(AWlogString(fmt.Sprintf("scanning service policies for updates")))

// 	// Iterate over each org in the BusinessPolManager and process all the business policies in that org
// 	for _, org := range w.BusinessPolManager.GetAllPolicyOrgs() {
// 		orgMap := w.BusinessPolManager.GetAllBusinessPolicyEntriesForOrg(org)
// 		if orgMap != nil {
// 			for bpName, bPol := range orgMap {
// 				if bPol.ServicePolicies != nil {
// 					for svcKey, _ := range bPol.ServicePolicies {
// 						if _, ok := updatedServicePols[svcKey]; !ok {
// 							servicePolicy, err := w.getServicePolicy(svcKey)
// 							if err != nil {
// 								glog.Errorf(AWlogString(fmt.Sprintf("Error getting service policy for %v, %v", svcKey, err)))
// 							} else if servicePolicy == nil {
// 								// delete the service policy from all the business policies that reference it.
// 								if err := w.BusinessPolManager.RemoveServicePolicy(org, bpName, svcKey); err != nil {
// 									glog.Errorf(AWlogString(fmt.Sprintf("Error deleting service policy %v in the business policy manager: %v", svcKey, err)))
// 								}
// 							} else {
// 								// update the service policy for all the business policies that reference it.
// 								if err := w.BusinessPolManager.AddServicePolicy(org, bpName, svcKey, servicePolicy); err != nil {
// 									glog.Errorf(AWlogString(fmt.Sprintf("Error updating service policy %v in the business policy manager: %v", svcKey, err)))
// 								}
// 							}
// 							updatedServicePols[svcKey] = 1
// 						}
// 					}
// 				}
// 			}
// 		}
// 	}

// 	glog.V(5).Infof(AWlogString(fmt.Sprintf("done scanning service policies for updates")))

// }

// // Get service policy
// func (w *AgreementBotWorker) getServicePolicy(svcId string) (*externalpolicy.ExternalPolicy, error) {

// 	servicePolicyHandler := exchange.GetHTTPServicePolicyWithIdHandler(w)
// 	servicePolicy, err := servicePolicyHandler(svcId)
// 	if err != nil {
// 		return nil, fmt.Errorf("error trying to query service policy for %v: %v", svcId, err)
// 	} else if servicePolicy == nil {
// 		return nil, nil
// 	} else {
// 		extPolicy := servicePolicy.GetExternalPolicy()
// 		return &extPolicy, nil
// 	}
// }

// Heartbeat to the database. This function is called by the database heartbeat subworker.
func (w *AgreementBotWorker) databaseHeartBeat() int {

	if err := w.db.HeartbeatPartition(); err != nil {
		glog.Errorf(AWlogString(fmt.Sprintf("Error heartbeating to the database, error: %v", err)))
	}

	return 0
}

// Ask the database to check for stale partitions and move them into our partition if one is found.
func (w *AgreementBotWorker) stalePartitions() int {

	if err := w.db.MovePartition(w.Config.GetPartitionStale()); err != nil {
		glog.Errorf(AWlogString(fmt.Sprintf("Error claiming an unowned partition, error: %v", err)))
	}
	return 0
}

// Ensure that the agbot's message key is still in its object in the exchange. If the agbot itself is missing,
// we will panic (that should not happen). If the key is missing (i.e. the current key is a zero length byte array)
// we will add our key back. If there is a key but it is just wrong, we will panic. This latter case could occur if
// multiple agbots are setup without sharing the same messaging key.
func (w *AgreementBotWorker) messageKeyCheck() int {

	glog.V(5).Infof(AWlogString(fmt.Sprintf("checking agbot message key")))

	key := exchange.CreateAgbotPublicKeyPatch(w.Config.AgreementBot.MessageKeyPath).PublicKey
	var resp interface{}
	resp = new(exchange.GetAgbotsResponse)
	targetURL := w.GetExchangeURL() + "orgs/" + exchange.GetOrg(w.GetExchangeId()) + "/agbots/" + exchange.GetId(w.GetExchangeId())
	for {
		if err, tpErr := exchange.InvokeExchange(w.httpClient, "GET", targetURL, w.GetExchangeId(), w.GetExchangeToken(), nil, &resp); err != nil {
			glog.Errorf(err.Error())
			return 0
		} else if tpErr != nil {
			glog.Warningf(tpErr.Error())
			time.Sleep(10 * time.Second)
			continue
		} else {

			// Got a response from the exchange. Make sure this agbot is in the response.
			ags := resp.(*exchange.GetAgbotsResponse).Agbots
			if agbot, there := ags[w.GetExchangeId()]; !there {
				msg := AWlogString(fmt.Sprintf("agbot %v not in GET response %v as expected", w.GetExchangeId(), ags))
				glog.Errorf(msg)
				panic(msg)

			} else if len(agbot.PublicKey) == 0 {

				// There is no message key in the exchange, this should not happen but we can fix it, so we will add it back in if we can.
				glog.Errorf(AWlogString(fmt.Sprintf("agbot message key is empty, adding it back in %v", key)))
				if err := w.registerPublicKey(); err != nil {
					msg := AWlogString(fmt.Sprintf("unable to register public key, error: %v", err))
					glog.Errorf(msg)
					panic(msg)
				}

			} else if !bytes.Equal(key, agbot.PublicKey) {

				// Make sure the message key in the exchange is our key. If not, exit quickly.
				msg := AWlogString(fmt.Sprintf("agbot message key has changed from %v to %v", key, agbot.PublicKey))
				glog.Errorf(msg)
				panic(msg)

			} else {
				glog.V(5).Infof(AWlogString(fmt.Sprintf("agbot message key is present")))
			}
			return 0

		}
	}

}

// ==========================================================================================================
// Utility functions

var AWlogString = func(v interface{}) string {
	return fmt.Sprintf("AgreementBotWorker %v", v)
}
