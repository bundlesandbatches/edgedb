#
# This source file is part of the EdgeDB open source project.
#
# Copyright 2008-present MagicStack Inc. and the EdgeDB authors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#


from __future__ import annotations
from typing import *

from edb import errors

from edb.edgeql import ast as qlast
from edb.edgeql import compiler as qlcompiler
from edb.edgeql import qltypes

from . import annos as s_anno
from . import delta as sd
from . import expr as s_expr
# from . import name as sn
from . import objects as so
from . import pointers as s_pointers
from . import types as s_types
from . import utils

if TYPE_CHECKING:
    from edb.schema import schema as s_schema


class Global(
    so.QualifiedObject,
    s_anno.AnnotationSubject,
    qlkind=qltypes.SchemaObjectClass.GLOBAL,
    data_safe=True,
):

    target = so.SchemaField(
        s_types.Type,
        default=None,
        compcoef=0.85,
        special_ddl_syntax=True,
    )

    required = so.SchemaField(
        bool,
        default=False,
        compcoef=0.909,
        special_ddl_syntax=True,
        describe_visibility=(
            so.DescribeVisibilityPolicy.SHOW_IF_EXPLICIT_OR_DERIVED
        ),
    )

    cardinality = so.SchemaField(
        qltypes.SchemaCardinality,
        default=qltypes.SchemaCardinality.One,
        compcoef=0.833,
        coerce=True,
        special_ddl_syntax=True,
        describe_visibility=(
            so.DescribeVisibilityPolicy.SHOW_IF_EXPLICIT_OR_DERIVED
        ),
    )

    # Computable globals have this set to an expression
    # defining them.
    expr = so.SchemaField(
        s_expr.Expression,
        default=None,
        coerce=True,
        compcoef=0.909,
        special_ddl_syntax=True,
    )

    default = so.SchemaField(
        s_expr.Expression,
        allow_ddl_set=True,
        default=None,
        coerce=True,
        compcoef=0.909,
    )

    def is_computable(self, schema: s_schema.Schema) -> bool:
        return bool(self.get_expr(schema))


class GlobalCommandContext(
    sd.ObjectCommandContext[so.Object],
    s_anno.AnnotationSubjectCommandContext
):
    pass


class GlobalCommand(
    sd.QualifiedObjectCommand[Global],
    context_class=GlobalCommandContext,
):
    def canonicalize_attributes(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
    ) -> s_schema.Schema:
        schema = super().canonicalize_attributes(schema, context)
        target_ref = self.get_local_attribute_value('target')
        inf_target_ref: Optional[s_types.TypeShell[s_types.Type]]

        # # When cardinality/required is altered, we need to force a
        # # reconsideration of expr if it exists in order to check
        # # it against the new specifier or compute them on a
        # # RESET. This is kind of unfortunate.
        # if (
        #     isinstance(self, sd.AlterObject)
        #     and (
        #         (
        #             self.has_attribute_value('cardinality')
        #             and not self.is_attribute_inherited('cardinality')
        #         ) or (
        #             self.has_attribute_value('required')
        #             and not self.is_attribute_inherited('required')
        #         )
        #     )
        #     and not self.has_attribute_value('expr')
        #     and (expr := self.scls.get_expr(schema)) is not None
        # ):
        #     self.set_attribute_value(
        #         'expr',
        #         s_expr.Expression.not_compiled(expr)
        #     )

        if isinstance(target_ref, s_pointers.ComputableRef):
            raise errors.SchemaDefinitionError(
                "Computable globals are not yet implemented",
                context=self.source_context,
            )
            # schema, inf_target_ref, base = self._parse_computable(
            #     target_ref.expr, schema, context)
        elif (self.get_local_attribute_value('expr')) is not None:
            raise errors.SchemaDefinitionError(
                "Computable globals are not yet implemented",
                context=self.source_context,
            )
            # schema = s_types.materialize_type_in_attribute(
            #     schema, context, self, 'target')
            # schema, inf_target_ref, base = self._parse_computable(
            #     expr.qlast, schema, context)
        # else:
        #     inf_target_ref = None

        # if inf_target_ref is not None:
        #     srcctx = self.get_attribute_source_context('target')
        #     self.set_attribute_value(
        #         'target',
        #         inf_target_ref,
        #         source_context=srcctx,
        #         computed=True,
        #     )

        schema = s_types.materialize_type_in_attribute(
            schema, context, self, 'target')

        return schema

    def validate_object(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
    ) -> None:
        scls = self.scls
        is_computable = scls.is_computable(schema)

        if not is_computable:
            if (
                scls.get_required(schema)
                and not scls.get_default(schema)
            ):
                raise errors.SchemaDefinitionError(
                    "Required globals must have a default",
                    context=self.source_context,
                )
            if scls.get_cardinality(schema) == qltypes.SchemaCardinality.Many:
                raise errors.SchemaDefinitionError(
                    "Only computed globals may be multi",
                    context=self.source_context,
                )

        default_expr = scls.get_default(schema)

        if default_expr is not None:
            from edb.ir import ast as irast
            if default_expr.irast is None:
                default_expr = default_expr.compiled(default_expr, schema)

            assert isinstance(default_expr.irast, irast.Statement)

            default_type = default_expr.irast.stype
            ptr_target = scls.get_target(schema)
            assert ptr_target is not None

            source_context = self.get_attribute_source_context('default')
            if not default_type.assignment_castable_to(ptr_target, schema):
                raise errors.SchemaDefinitionError(
                    f'default expression is of invalid type: '
                    f'{default_type.get_displayname(schema)}, '
                    f'expected {ptr_target.get_displayname(schema)}',
                    context=source_context,
                )

            # "required" status of defaults should not be enforced
            # because it's impossible to actually guarantee that any
            # SELECT involving a path is non-empty
            # XXX: we could, though...
            ptr_cardinality = scls.get_cardinality(schema)
            default_required, default_cardinality = \
                default_expr.irast.cardinality.to_schema_value()

            if (ptr_cardinality is qltypes.SchemaCardinality.One
                    and default_cardinality != ptr_cardinality):
                raise errors.SchemaDefinitionError(
                    f'possibly more than one element returned by '
                    f'the default expression for '
                    f'{scls.get_verbosename(schema)} declared as '
                    f"'single'",
                    context=source_context,
                )

            # XXX: volatility?

    def get_dummy_expr_field_value(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
        field: so.Field[Any],
        value: Any,
    ) -> Optional[s_expr.Expression]:
        if field.name == 'expr':
            return None
        elif field.name == 'default':
            return None
        else:
            raise NotImplementedError(f'unhandled field {field.name!r}')

    def compile_expr_field(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
        field: so.Field[Any],
        value: s_expr.Expression,
        track_schema_ref_exprs: bool=False,
    ) -> s_expr.Expression:
        if field.name in {'default', 'expr'}:
            ptr_name = self.get_verbosename()
            in_ddl_context_name = None
            if field.name == 'expr':
                in_ddl_context_name = f'computed {ptr_name}'

            return type(value).compiled(
                value,
                schema=schema,
                options=qlcompiler.CompilerOptions(
                    modaliases=context.modaliases,
                    schema_object_context=self.get_schema_metaclass(),
                    apply_query_rewrites=not context.stdmode,
                    track_schema_ref_exprs=track_schema_ref_exprs,
                    in_ddl_context_name=in_ddl_context_name,
                ),
            )
        else:
            return super().compile_expr_field(
                schema, context, field, value, track_schema_ref_exprs)


class CreateGlobal(
    GlobalCommand,
    sd.CreateObject[Global],
):
    astnode = qlast.CreateGlobal

    def get_ast_attr_for_field(
        self,
        field: str,
        astnode: Type[qlast.DDLOperation],
    ) -> Optional[str]:
        if (
            field == 'required'
            and issubclass(astnode, qlast.CreateGlobal)
        ):
            return 'is_required'
        elif (
            field == 'cardinality'
            and issubclass(astnode, qlast.CreateGlobal)
        ):
            return 'cardinality'
        else:
            return super().get_ast_attr_for_field(field, astnode)

    def _apply_field_ast(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
        node: qlast.DDLOperation,
        op: sd.AlterObjectProperty,
    ) -> None:
        assert isinstance(node, qlast.CreateGlobal)
        if op.property == 'target':
            if not node.target:
                expr = self.get_attribute_value('expr')
                if expr is not None:
                    node.target = expr.qlast
                else:
                    t = op.new_value
                    assert isinstance(t, (so.Object, so.ObjectShell))
                    node.target = utils.typeref_to_ast(schema, t)
        else:
            super()._apply_field_ast(schema, context, node, op)

    @classmethod
    def _cmd_tree_from_ast(
        cls,
        schema: s_schema.Schema,
        astnode: qlast.DDLOperation,
        context: sd.CommandContext,
    ) -> sd.Command:
        cmd = super()._cmd_tree_from_ast(schema, astnode, context)

        assert isinstance(astnode, qlast.CreateGlobal)
        assert isinstance(cmd, GlobalCommand)

        if astnode.is_required is not None:
            cmd.set_attribute_value(
                'required',
                astnode.is_required,
                source_context=astnode.context,
            )

        if astnode.cardinality is not None:
            cmd.set_attribute_value(
                'cardinality',
                astnode.cardinality,
                source_context=astnode.context,
            )

        # FIXME: this is an approximate solution
        targets = qlast.get_targets(astnode.target)
        target_ref: Union[
            None, s_types.TypeShell[s_types.Type], s_pointers.ComputableRef]

        if len(targets) > 1:
            raise errors.SchemaDefinitionError(
                "Can't specify a union target for a global???",
                context=astnode.context,
            )
        else:
            target_expr = targets[0]
            if isinstance(target_expr, qlast.TypeName):
                target_ref = utils.ast_to_type_shell(
                    target_expr,
                    metaclass=s_types.Type,
                    modaliases=context.modaliases,
                    schema=schema,
                )
            else:
                # computable
                qlcompiler.normalize(
                    target_expr,
                    schema=schema,
                    modaliases=context.modaliases
                )
                target_ref = s_pointers.ComputableRef(target_expr)

        assert astnode.target is not None
        cmd.set_attribute_value(
            'target',
            target_ref,
            source_context=astnode.target.context,
        )

        return cmd


class RenameGlobal(
    GlobalCommand,
    sd.RenameObject[Global],
):
    pass


class AlterGlobal(
    GlobalCommand,
    sd.AlterObject[Global],
):
    astnode = qlast.AlterGlobal


class DeleteGlobal(
    GlobalCommand,
    sd.DeleteObject[Global],
):
    astnode = qlast.DropGlobal
