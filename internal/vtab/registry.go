// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package vtab

import (
	"fmt"
	"sync"
)

// ModuleRegistry manages the registration and lookup of virtual table modules.
// This provides a centralized registry for all virtual table implementations.
type ModuleRegistry struct {
	mu      sync.RWMutex
	modules map[string]Module
}

// NewModuleRegistry creates a new module registry.
func NewModuleRegistry() *ModuleRegistry {
	return &ModuleRegistry{
		modules: make(map[string]Module),
	}
}

// RegisterModule registers a virtual table module with the given name.
// If a module with the same name already exists, it returns an error.
func (r *ModuleRegistry) RegisterModule(name string, module Module) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if _, exists := r.modules[name]; exists {
		return fmt.Errorf("virtual table module %q already registered", name)
	}

	r.modules[name] = module
	return nil
}

// UnregisterModule removes a virtual table module from the registry.
// Returns an error if the module is not registered.
func (r *ModuleRegistry) UnregisterModule(name string) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if _, exists := r.modules[name]; !exists {
		return fmt.Errorf("virtual table module %q not registered", name)
	}

	delete(r.modules, name)
	return nil
}

// GetModule retrieves a virtual table module by name.
// Returns nil if the module is not registered.
func (r *ModuleRegistry) GetModule(name string) Module {
	r.mu.RLock()
	defer r.mu.RUnlock()

	return r.modules[name]
}

// HasModule checks if a module with the given name is registered.
func (r *ModuleRegistry) HasModule(name string) bool {
	r.mu.RLock()
	defer r.mu.RUnlock()

	_, exists := r.modules[name]
	return exists
}

// ListModules returns a list of all registered module names.
func (r *ModuleRegistry) ListModules() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()

	names := make([]string, 0, len(r.modules))
	for name := range r.modules {
		names = append(names, name)
	}
	return names
}

// Clear removes all registered modules from the registry.
func (r *ModuleRegistry) Clear() {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.modules = make(map[string]Module)
}

// defaultRegistry is the global default module registry.
var defaultRegistry = NewModuleRegistry()

// RegisterModule registers a module in the default global registry.
func RegisterModule(name string, module Module) error {
	return defaultRegistry.RegisterModule(name, module)
}

// UnregisterModule removes a module from the default global registry.
func UnregisterModule(name string) error {
	return defaultRegistry.UnregisterModule(name)
}

// GetModule retrieves a module from the default global registry.
func GetModule(name string) Module {
	return defaultRegistry.GetModule(name)
}

// HasModule checks if a module exists in the default global registry.
func HasModule(name string) bool {
	return defaultRegistry.HasModule(name)
}

// ListModules returns all module names from the default global registry.
func ListModules() []string {
	return defaultRegistry.ListModules()
}

// DefaultRegistry returns the default global module registry.
func DefaultRegistry() *ModuleRegistry {
	return defaultRegistry
}
