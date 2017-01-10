package ai.grakn.graql.internal.reasoner.atom.binary;

import ai.grakn.graql.admin.VarAdmin;
import ai.grakn.graql.VarName;
import ai.grakn.graql.internal.reasoner.atom.AtomicFactory;
import ai.grakn.graql.internal.reasoner.atom.predicate.Predicate;
import ai.grakn.graql.internal.reasoner.query.Query;

import java.util.Map;

/**
 *
 * <p>
 * Base implementation for binary atoms with single predicate.
 * </p>
 *
 * @author Kasper Piskorski
 *
 */
public abstract class Binary extends BinaryBase {
    private Predicate predicate = null;

    protected Binary(VarAdmin pattern, Predicate p, Query par) {
        super(pattern, par);
        this.predicate = p;
        this.typeId = extractTypeId(atomPattern.asVar());
    }

    protected Binary(Binary a) {
        super(a);
        this.predicate = a.getPredicate() != null ? (Predicate) AtomicFactory.create(a.getPredicate(), getParentQuery()) : null;
        this.typeId = extractTypeId(atomPattern.asVar());
    }

    protected abstract String extractTypeId(VarAdmin var);

    @Override
    public void setParentQuery(Query q) {
        super.setParentQuery(q);
        if (predicate != null) predicate.setParentQuery(q);
    }

    public Predicate getPredicate() { return predicate;}
    protected void setPredicate(Predicate p) { predicate = p;}

    @Override
    protected boolean predicatesEquivalent(BinaryBase atom) {
        Predicate pred = getPredicate();
        Predicate objPredicate = ((Binary) atom).getPredicate();
        return (pred == null && objPredicate == null)
                || ((pred != null && objPredicate != null) && pred.isEquivalent(objPredicate));
    }

    @Override
    public int equivalenceHashCode() {
        int hashCode = 1;
        hashCode = hashCode * 37 + this.typeId.hashCode();
        hashCode = hashCode * 37 + (predicate != null ? predicate.equivalenceHashCode() : 0);
        return hashCode;
    }

    @Override
    public boolean isValueUserDefinedName() {
        return predicate == null && !getValueVariable().getValue().isEmpty();
    }

    @Override
    public void unify (Map<VarName, VarName> unifiers) {
        super.unify(unifiers);
        if (predicate != null) predicate.unify(unifiers);
    }
}
