package matchcache

import(
	"encoding/base64"
	"github.com/open-horizon/anax/cutil"
	"github.com/open-horizon/anax/externalpolicy"
)

type NodePolicyHash []byte

type NodePolicyHashString string

func (n NodePolicyHash) toHashString() NodePolicyHashString {
	return NodePolicyHashString(base64.StdEncoding.EncodeToString(n))
}

func HashNodePolicy(np *externalpolicy.ExternalPolicy) (NodePolicyHashString, error) {
	nphs := NodePolicyHashString("")
	npb, err := cutil.HashObject(np)
	if err != nil {
		return nphs, err
	}

	return NodePolicyHash(npb).toHashString(), nil

}