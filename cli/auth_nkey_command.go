// Copyright 2023-2026 The NATS Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package cli

import (
	"crypto/rand"
	"encoding/base64"
	"errors"
	"fmt"
	"os"
	"strings"

	"github.com/nats-io/natscli/internal/fips"
	iu "github.com/nats-io/natscli/internal/util"

	"github.com/nats-io/nkeys"
	"github.com/spf13/cobra"
)

type authNKCommand struct {
	keyType        string
	pubOut         bool
	entropySource  string
	outFile        string
	keyFile        string
	dataFile       string
	signFile       string
	counterpartKey string
	useB64         bool
}

func configureAuthNkeyCommand(auth commandHost) {
	c := &authNKCommand{}

	nk := addCommand(auth, "nkey", "Create and Use NKeys")
	nk.Aliases = []string{"nk"}

	nkGen := addCommand(nk, "gen", "Generates NKeys")
	nkGen.RunE = c.genAction
	addArgEnum(nkGen, "type", "Type of key to generate", true, "user", "account", "server", "cluster", "operator", "curve", "x25519")
	nkGen.Flags().BoolVar(&c.pubOut, "public", false, "Output the public key")
	nkGen.Flags().Var(newExistingFileValue(&c.entropySource), "entropy", "Source of entropy eg. /dev/urandom")
	nkGen.Flags().StringVar(&c.outFile, "output", "", "Write the key to a file")

	nkShow := addCommand(nk, "show", "Show the public key")
	nkShow.RunE = c.showAction
	addArg(nkShow, "key", "File containing NKey to act on", true, "string")

	nkSign := addCommand(nk, "sign", "Signs data using NKeys")
	nkSign.RunE = c.signAction
	addArg(nkSign, "file", "File to sign", true, "string")
	addArg(nkSign, "key", "File containing NKey to sign with", true, "string")

	nkVerify := addCommand(nk, "verify", "Verify signed data")
	nkVerify.RunE = c.verifyAction
	addArg(nkVerify, "file", "File containing the data to check", true, "string")
	addArg(nkVerify, "signature", "File containing the signature", true, "string")
	addArg(nkVerify, "key", "File containing NKey to use for verification", true, "string")

	nkSeal := addCommand(nk, "seal", "Encrypts a file using NKeys")
	nkSeal.Aliases = []string{"encrypt", "enc"}
	nkSeal.RunE = c.sealAction
	addArg(nkSeal, "file", "File to encrypt", true, "string")
	addArg(nkSeal, "key", "File containing NKey to encrypt with", true, "string")
	addArg(nkSeal, "recipient", "Public XKey of recipient", true, "string")
	nkSeal.Flags().StringVar(&c.outFile, "output", "", "Write the encrypted data to a file")
	negatableBoolVar(nkSeal, &c.useB64, "b64", true, "Write base64 encoded data")

	nkOpen := addCommand(nk, "unseal", "Decrypts a file using NKeys")
	nkOpen.Aliases = []string{"open", "decrypt", "dec"}
	nkOpen.RunE = c.unsealAction
	addArg(nkOpen, "file", "File to decrypt", true, "string")
	addArg(nkOpen, "key", "File containing NKey to decrypt with", true, "string")
	addArg(nkOpen, "sender", "Public XKey of sender", true, "string")
	nkOpen.Flags().StringVar(&c.outFile, "output", "", "Write the decrypted data to a file")
	negatableBoolVar(nkOpen, &c.useB64, "b64", true, "Read data in as base64 encoded")
}

func (c *authNKCommand) showAction(_ *cobra.Command, args []string) error {
	c.keyFile = args[0]

	seed, err := iu.ReadKeyFile(c.keyFile)
	if err != nil {
		return err
	}

	kp, err := nkeys.FromSeed(seed)
	if err != nil {
		log.Fatal(err)
	}
	pub, err := kp.PublicKey()
	if err != nil {
		return err
	}

	fmt.Println(pub)

	return nil
}

func (c *authNKCommand) genAction(_ *cobra.Command, args []string) error {
	c.keyType = args[0]

	prefix, err := c.preForType(c.keyType)
	if err != nil {
		return err
	}

	ef := rand.Reader
	if c.entropySource != "" {
		r, err := os.Open(c.entropySource)
		if err != nil {
			return fmt.Errorf("could not use custom entropy source: %w", err)
		}

		ef = r
	}

	var kp nkeys.KeyPair

	if prefix == nkeys.PrefixByteCurve {
		if fips.Enabled() {
			return fips.DisabledError("nats auth nkey gen curve", "X25519")
		}
		kp, err = nkeys.CreateCurveKeysWithRand(ef)
	} else {
		kp, err = nkeys.CreatePairWithRand(prefix, ef)
	}
	if err != nil {
		return fmt.Errorf("could not create %q: %w", prefix, err)
	}

	seed, err := kp.Seed()
	if err != nil {
		return err
	}

	out := os.Stdout
	if c.outFile != "" {
		out, err = os.Create(c.outFile)
		if err != nil {
			return err
		}
		defer out.Close()
	}

	_, err = fmt.Fprintln(out, string(seed))
	if err != nil {
		return err
	}

	if c.pubOut {
		pk, err := kp.PublicKey()
		if err != nil {
			return err
		}

		_, err = fmt.Fprintln(out, pk)
		if err != nil {
			return err
		}
	}

	return nil
}

func (c *authNKCommand) preForType(keyType string) (nkeys.PrefixByte, error) {
	switch strings.ToLower(keyType) {
	case "user":
		return nkeys.PrefixByteUser, nil
	case "account":
		return nkeys.PrefixByteAccount, nil
	case "server":
		return nkeys.PrefixByteServer, nil
	case "cluster":
		return nkeys.PrefixByteCluster, nil
	case "operator":
		return nkeys.PrefixByteOperator, nil
	case "curve", "x25519":
		if fips.Enabled() {
			return nkeys.PrefixByte(0), fips.DisabledError("curve nkeys", "X25519")
		}
		return nkeys.PrefixByteCurve, nil
	default:
		return nkeys.PrefixByte(0), fmt.Errorf("unknown prefix type %q", keyType)
	}
}

func (c *authNKCommand) signAction(_ *cobra.Command, args []string) error {
	c.dataFile = args[0]
	c.keyFile = args[1]

	seed, err := iu.ReadKeyFile(c.keyFile)
	if err != nil {
		return err
	}

	kp, err := nkeys.FromSeed(seed)
	if err != nil {
		return err
	}

	content, err := os.ReadFile(c.dataFile)
	if err != nil {
		return err
	}

	sigBytes, err := kp.Sign(content)
	if err != nil {
		return err
	}

	fmt.Println(base64.RawURLEncoding.EncodeToString(sigBytes))

	return nil
}

func (c *authNKCommand) verifyAction(_ *cobra.Command, args []string) error {
	c.dataFile = args[0]
	c.signFile = args[1]
	c.keyFile = args[2]

	var err error
	var kp nkeys.KeyPair

	keyData, err := iu.ReadKeyFile(c.keyFile)
	if err != nil {
		return err
	}

	// try it as public, then as seed
	kp, err = nkeys.FromPublicKey(string(keyData))
	if errors.Is(err, nkeys.ErrInvalidPublicKey) {
		kp, err = nkeys.FromSeed(keyData)
	}
	if err != nil {
		return err
	}

	content, err := os.ReadFile(c.dataFile)
	if err != nil {
		return err
	}

	sigEnc, err := os.ReadFile(c.signFile)
	if err != nil {
		return err
	}

	sig, err := base64.RawURLEncoding.DecodeString(string(sigEnc))
	if err != nil {
		return err
	}

	if err := kp.Verify(content, sig); err != nil {
		return err
	}

	fmt.Println("Verified OK")

	return nil
}

func (c *authNKCommand) sealAction(_ *cobra.Command, args []string) error {
	c.dataFile = args[0]
	c.keyFile = args[1]
	c.counterpartKey = args[2]

	keyData, err := iu.ReadKeyFile(c.keyFile)
	if err != nil {
		return err
	}

	// try it as public, then as seed
	kp, err := nkeys.FromPublicKey(string(keyData))
	if errors.Is(err, nkeys.ErrInvalidPublicKey) {
		kp, err = nkeys.FromSeed(keyData)
	}
	if err != nil {
		return err
	}

	content, err := os.ReadFile(c.dataFile)
	if err != nil {
		return err
	}

	if !nkeys.IsValidPublicCurveKey(c.counterpartKey) {
		return errors.New("invalid public key provided")
	}

	if fips.Enabled() {
		return fips.DisabledError("nats auth nkey seal", "X25519")
	}

	encryptedData, err := kp.Seal(content, c.counterpartKey)
	if err != nil {
		return err
	}

	if c.useB64 {
		encryptedData = []byte(base64.StdEncoding.EncodeToString(encryptedData))
	}

	if c.outFile == "" {
		fmt.Println(string(encryptedData))
		return nil
	}

	f, err := os.Create(c.outFile)
	if err != nil {
		return err
	}
	defer f.Close()

	_, err = f.Write(encryptedData)
	if err != nil {
		return err
	}

	return nil
}

func (c *authNKCommand) unsealAction(_ *cobra.Command, args []string) error {
	c.dataFile = args[0]
	c.keyFile = args[1]
	c.counterpartKey = args[2]

	keyData, err := iu.ReadKeyFile(c.keyFile)
	if err != nil {
		return err
	}

	// try it as public, then as seed
	kp, err := nkeys.FromPublicKey(string(keyData))
	if errors.Is(err, nkeys.ErrInvalidPublicKey) {
		kp, err = nkeys.FromSeed(keyData)
	}
	if err != nil {
		return err
	}

	content, err := os.ReadFile(c.dataFile)
	if err != nil {
		return err
	}

	if c.useB64 {
		var err error
		content, err = base64.StdEncoding.DecodeString(string(content))
		if err != nil {
			return err
		}
	}

	if !nkeys.IsValidPublicCurveKey(c.counterpartKey) {
		return errors.New("invalid public key provided")
	}

	if fips.Enabled() {
		return fips.DisabledError("nats auth nkey unseal", "X25519")
	}

	decryptedData, err := kp.Open(content, c.counterpartKey)
	if err != nil {
		return err
	}

	if c.outFile == "" {
		fmt.Println(string(decryptedData))
		return nil
	}

	f, err := os.Create(c.outFile)
	if err != nil {
		return err
	}
	defer f.Close()

	_, err = f.Write(decryptedData)
	if err != nil {
		return err
	}

	return nil
}
