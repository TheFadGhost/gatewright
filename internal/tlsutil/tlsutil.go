// Package tlsutil generates throwaway self-signed certificates for tests and
// local demos, per DESIGN.md §8: TLS material is created at test time by
// tooling in this repo and never committed.
package tlsutil

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// GenerateSelfSignedCert writes a fresh ECDSA P-256 self-signed certificate
// and its private key into dir and returns their file paths.
//
//   - Validity: 7 days (NotBefore backdated one hour for clock skew).
//   - SANs: every entry of hosts that parses as an IP becomes an IP SAN,
//     everything else a DNS SAN; "127.0.0.1" and "localhost" are always
//     included so loopback clients work without extra configuration.
//   - The certificate doubles as its own CA (IsCA, KeyUsageCertSign) so
//     clients can trust it directly as a root.
//   - Both PEM files are written with 0600 permissions.
//
// No external dependencies; stdlib crypto only.
func GenerateSelfSignedCert(dir string, hosts []string) (certPath, keyPath string, err error) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return "", "", fmt.Errorf("tlsutil: key generation failed: %w", err)
	}

	serialLimit := new(big.Int).Lsh(big.NewInt(1), 128)
	serial, err := rand.Int(rand.Reader, serialLimit)
	if err != nil {
		return "", "", fmt.Errorf("tlsutil: serial generation failed: %w", err)
	}

	sanHosts := make([]string, 0, len(hosts)+2)
	sanIPs := make([]net.IP, 0, len(hosts)+2)
	addHost := func(h string) {
		h = strings.ToLower(strings.TrimSpace(h))
		if h == "" {
			return
		}
		if ip := net.ParseIP(h); ip != nil {
			for _, existing := range sanIPs {
				if existing.Equal(ip) {
					return
				}
			}
			sanIPs = append(sanIPs, ip)
			return
		}
		for _, existing := range sanHosts {
			if existing == h {
				return
			}
		}
		sanHosts = append(sanHosts, h)
	}
	for _, h := range hosts {
		addHost(h)
	}
	addHost("127.0.0.1")
	addHost("localhost")

	now := time.Now()
	tmpl := &x509.Certificate{
		SerialNumber:          serial,
		Subject:               pkix.Name{CommonName: "gatewright-test", Organization: []string{"Gatewright"}},
		NotBefore:             now.Add(-time.Hour),
		NotAfter:              now.Add(7 * 24 * time.Hour),
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment | x509.KeyUsageCertSign,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true,
		IsCA:                  true,
		DNSNames:              sanHosts,
		IPAddresses:           sanIPs,
	}

	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		return "", "", fmt.Errorf("tlsutil: certificate creation failed: %w", err)
	}

	if err := os.MkdirAll(dir, 0o755); err != nil {
		return "", "", fmt.Errorf("tlsutil: cannot create %q: %w", dir, err)
	}
	certPath = filepath.Join(dir, "cert.pem")
	keyPath = filepath.Join(dir, "key.pem")

	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	if err := os.WriteFile(certPath, certPEM, 0o600); err != nil {
		return "", "", fmt.Errorf("tlsutil: cannot write certificate: %w", err)
	}
	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		return "", "", fmt.Errorf("tlsutil: key marshalling failed: %w", err)
	}
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER})
	if err := os.WriteFile(keyPath, keyPEM, 0o600); err != nil {
		return "", "", fmt.Errorf("tlsutil: cannot write private key: %w", err)
	}
	return certPath, keyPath, nil
}
