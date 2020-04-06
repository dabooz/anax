package matchcache

import ()

// Types used throughout the match cache
type DeploymentPolicyId string

type DeploymentPolicyMap map[DeploymentPolicyId]bool

func (d DeploymentPolicyId) AsString() string {
	return string(d)
}
