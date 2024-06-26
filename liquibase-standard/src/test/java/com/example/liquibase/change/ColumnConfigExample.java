package com.example.liquibase.change;

import liquibase.serializer.AbstractLiquibaseSerializable;

public class ColumnConfigExample extends AbstractLiquibaseSerializable {
    private String name;
    private String type;
    private Boolean nullable;
    private IdentityConfig identity;
    private DefaultConstraintConfig defaultConstraint;
    private ComputedConfig computed;

    @Override
    public String getSerializedObjectNamespace() {
        return GENERIC_CHANGELOG_EXTENSION_NAMESPACE;
    }

    @Override
    public String getSerializedObjectName() {
        return "column";
    }

    public String getName() {
        return name;
    }

    public ColumnConfigExample setName(String name) {
        this.name = name;
        return this;
    }

    public String getType() {
        return type;
    }

    public ColumnConfigExample setType(String type) {
        this.type = type;
        return this;
    }

    public Boolean getNullable() {
        return nullable;
    }

    public ColumnConfigExample setNullable(Boolean nullable) {
        this.nullable = nullable;
        return this;
    }

    public IdentityConfig getIdentity() {
        return identity;
    }

    public ColumnConfigExample setIdentity(IdentityConfig identity) {
        this.identity = identity;
        return this;
    }

    public DefaultConstraintConfig getDefaultConstraint() {
        return defaultConstraint;
    }

    public ColumnConfigExample setDefaultConstraint(DefaultConstraintConfig defaultConstraint) {
        this.defaultConstraint = defaultConstraint;
        return this;
    }

    public ComputedConfig getComputed() {
        return computed;
    }

    public ColumnConfigExample setComputed(ComputedConfig computed) {
        this.computed = computed;
        return this;
    }
}
