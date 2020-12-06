from math import ceil
from operator import attrgetter

from django.db import IntegrityError, NotSupportedError, connection, OperationalError
from django.db.models import FileField, Value
from django.db.models.functions import Lower
from django.test import (
    TestCase, override_settings, skipIfDBFeature, skipUnlessDBFeature, skipUnlessAnyDBFeature
)

from .models import (
    BigAutoFieldModel, Country, NoFields, NullableFields, Pizzeria,
    ProxyCountry, ProxyMultiCountry, ProxyMultiProxyCountry, ProxyProxyCountry,
    Restaurant, SmallAutoFieldModel, State, TwoFields, UniqueTogether,
    UniqueTwo, UpsertConflict,
)


class BulkCreateTests(TestCase):
    def setUp(self):
        self.data = [
            Country(name="United States of America", iso_two_letter="US"),
            Country(name="The Netherlands", iso_two_letter="NL"),
            Country(name="Germany", iso_two_letter="DE"),
            Country(name="Czech Republic", iso_two_letter="CZ")
        ]

    def test_simple(self):
        created = Country.objects.bulk_create(self.data)
        self.assertEqual(created, self.data)
        self.assertQuerysetEqual(Country.objects.order_by("-name"), [
            "United States of America", "The Netherlands", "Germany", "Czech Republic"
        ], attrgetter("name"))

        created = Country.objects.bulk_create([])
        self.assertEqual(created, [])
        self.assertEqual(Country.objects.count(), 4)

    @skipUnlessDBFeature('has_bulk_insert')
    def test_efficiency(self):
        with self.assertNumQueries(1):
            Country.objects.bulk_create(self.data)

    @skipUnlessDBFeature('has_bulk_insert')
    def test_long_non_ascii_text(self):
        """
        Inserting non-ASCII values with a length in the range 2001 to 4000
        characters, i.e. 4002 to 8000 bytes, must be set as a CLOB on Oracle
        (#22144).
        """
        Country.objects.bulk_create([Country(description='Ж' * 3000)])
        self.assertEqual(Country.objects.count(), 1)

    @skipUnlessDBFeature('has_bulk_insert')
    def test_long_and_short_text(self):
        Country.objects.bulk_create([
            Country(description='a' * 4001),
            Country(description='a'),
            Country(description='Ж' * 2001),
            Country(description='Ж'),
        ])
        self.assertEqual(Country.objects.count(), 4)

    def test_multi_table_inheritance_unsupported(self):
        expected_message = "Can't bulk create a multi-table inherited model"
        with self.assertRaisesMessage(ValueError, expected_message):
            Pizzeria.objects.bulk_create([
                Pizzeria(name="The Art of Pizza"),
            ])
        with self.assertRaisesMessage(ValueError, expected_message):
            ProxyMultiCountry.objects.bulk_create([
                ProxyMultiCountry(name="Fillory", iso_two_letter="FL"),
            ])
        with self.assertRaisesMessage(ValueError, expected_message):
            ProxyMultiProxyCountry.objects.bulk_create([
                ProxyMultiProxyCountry(name="Fillory", iso_two_letter="FL"),
            ])

    def test_proxy_inheritance_supported(self):
        ProxyCountry.objects.bulk_create([
            ProxyCountry(name="Qwghlm", iso_two_letter="QW"),
            Country(name="Tortall", iso_two_letter="TA"),
        ])
        self.assertQuerysetEqual(ProxyCountry.objects.all(), {
            "Qwghlm", "Tortall"
        }, attrgetter("name"), ordered=False)

        ProxyProxyCountry.objects.bulk_create([
            ProxyProxyCountry(name="Netherlands", iso_two_letter="NT"),
        ])
        self.assertQuerysetEqual(ProxyProxyCountry.objects.all(), {
            "Qwghlm", "Tortall", "Netherlands",
        }, attrgetter("name"), ordered=False)

    def test_non_auto_increment_pk(self):
        State.objects.bulk_create([
            State(two_letter_code=s)
            for s in ["IL", "NY", "CA", "ME"]
        ])
        self.assertQuerysetEqual(State.objects.order_by("two_letter_code"), [
            "CA", "IL", "ME", "NY",
        ], attrgetter("two_letter_code"))

    @skipUnlessDBFeature('has_bulk_insert')
    def test_non_auto_increment_pk_efficiency(self):
        with self.assertNumQueries(1):
            State.objects.bulk_create([
                State(two_letter_code=s)
                for s in ["IL", "NY", "CA", "ME"]
            ])
        self.assertQuerysetEqual(State.objects.order_by("two_letter_code"), [
            "CA", "IL", "ME", "NY",
        ], attrgetter("two_letter_code"))

    @skipIfDBFeature('allows_auto_pk_0')
    def test_zero_as_autoval(self):
        """
        Zero as id for AutoField should raise exception in MySQL, because MySQL
        does not allow zero for automatic primary key if the
        NO_AUTO_VALUE_ON_ZERO SQL mode is not enabled.
        """
        valid_country = Country(name='Germany', iso_two_letter='DE')
        invalid_country = Country(id=0, name='Poland', iso_two_letter='PL')
        msg = 'The database backend does not accept 0 as a value for AutoField.'
        with self.assertRaisesMessage(ValueError, msg):
            Country.objects.bulk_create([valid_country, invalid_country])

    def test_batch_same_vals(self):
        # SQLite had a problem where all the same-valued models were
        # collapsed to one insert.
        Restaurant.objects.bulk_create([
            Restaurant(name='foo') for i in range(0, 2)
        ])
        self.assertEqual(Restaurant.objects.count(), 2)

    def test_large_batch(self):
        TwoFields.objects.bulk_create([
            TwoFields(f1=i, f2=i + 1) for i in range(0, 1001)
        ])
        self.assertEqual(TwoFields.objects.count(), 1001)
        self.assertEqual(
            TwoFields.objects.filter(f1__gte=450, f1__lte=550).count(),
            101)
        self.assertEqual(TwoFields.objects.filter(f2__gte=901).count(), 101)

    @skipUnlessDBFeature('has_bulk_insert')
    def test_large_single_field_batch(self):
        # SQLite had a problem with more than 500 UNIONed selects in single
        # query.
        Restaurant.objects.bulk_create([
            Restaurant() for i in range(0, 501)
        ])

    @skipUnlessDBFeature('has_bulk_insert')
    def test_large_batch_efficiency(self):
        with override_settings(DEBUG=True):
            connection.queries_log.clear()
            TwoFields.objects.bulk_create([
                TwoFields(f1=i, f2=i + 1) for i in range(0, 1001)
            ])
            self.assertLess(len(connection.queries), 10)

    def test_large_batch_mixed(self):
        """
        Test inserting a large batch with objects having primary key set
        mixed together with objects without PK set.
        """
        TwoFields.objects.bulk_create([
            TwoFields(id=i if i % 2 == 0 else None, f1=i, f2=i + 1)
            for i in range(100000, 101000)
        ])
        self.assertEqual(TwoFields.objects.count(), 1000)
        # We can't assume much about the ID's created, except that the above
        # created IDs must exist.
        id_range = range(100000, 101000, 2)
        self.assertEqual(TwoFields.objects.filter(id__in=id_range).count(), 500)
        self.assertEqual(TwoFields.objects.exclude(id__in=id_range).count(), 500)

    @skipUnlessDBFeature('has_bulk_insert')
    def test_large_batch_mixed_efficiency(self):
        """
        Test inserting a large batch with objects having primary key set
        mixed together with objects without PK set.
        """
        with override_settings(DEBUG=True):
            connection.queries_log.clear()
            TwoFields.objects.bulk_create([
                TwoFields(id=i if i % 2 == 0 else None, f1=i, f2=i + 1)
                for i in range(100000, 101000)])
            self.assertLess(len(connection.queries), 10)

    def test_explicit_batch_size(self):
        objs = [TwoFields(f1=i, f2=i) for i in range(0, 4)]
        num_objs = len(objs)
        TwoFields.objects.bulk_create(objs, batch_size=1)
        self.assertEqual(TwoFields.objects.count(), num_objs)
        TwoFields.objects.all().delete()
        TwoFields.objects.bulk_create(objs, batch_size=2)
        self.assertEqual(TwoFields.objects.count(), num_objs)
        TwoFields.objects.all().delete()
        TwoFields.objects.bulk_create(objs, batch_size=3)
        self.assertEqual(TwoFields.objects.count(), num_objs)
        TwoFields.objects.all().delete()
        TwoFields.objects.bulk_create(objs, batch_size=num_objs)
        self.assertEqual(TwoFields.objects.count(), num_objs)

    def test_empty_model(self):
        NoFields.objects.bulk_create([NoFields() for i in range(2)])
        self.assertEqual(NoFields.objects.count(), 2)

    @skipUnlessDBFeature('has_bulk_insert')
    def test_explicit_batch_size_efficiency(self):
        objs = [TwoFields(f1=i, f2=i) for i in range(0, 100)]
        with self.assertNumQueries(2):
            TwoFields.objects.bulk_create(objs, 50)
        TwoFields.objects.all().delete()
        with self.assertNumQueries(1):
            TwoFields.objects.bulk_create(objs, len(objs))

    @skipUnlessDBFeature('has_bulk_insert')
    def test_explicit_batch_size_respects_max_batch_size(self):
        objs = [Country() for i in range(1000)]
        fields = ['name', 'iso_two_letter', 'description']
        max_batch_size = max(connection.ops.bulk_batch_size(fields, objs), 1)
        with self.assertNumQueries(ceil(len(objs) / max_batch_size)):
            Country.objects.bulk_create(objs, batch_size=max_batch_size + 1)

    @skipUnlessDBFeature('has_bulk_insert')
    def test_bulk_insert_expressions(self):
        Restaurant.objects.bulk_create([
            Restaurant(name="Sam's Shake Shack"),
            Restaurant(name=Lower(Value("Betty's Beetroot Bar")))
        ])
        bbb = Restaurant.objects.filter(name="betty's beetroot bar")
        self.assertEqual(bbb.count(), 1)

    @skipUnlessDBFeature('has_bulk_insert')
    def test_bulk_insert_nullable_fields(self):
        fk_to_auto_fields = {
            'auto_field': NoFields.objects.create(),
            'small_auto_field': SmallAutoFieldModel.objects.create(),
            'big_auto_field': BigAutoFieldModel.objects.create(),
        }
        # NULL can be mixed with other values in nullable fields
        nullable_fields = [field for field in NullableFields._meta.get_fields() if field.name != 'id']
        NullableFields.objects.bulk_create([
            NullableFields(**{**fk_to_auto_fields, field.name: None})
            for field in nullable_fields
        ])
        self.assertEqual(NullableFields.objects.count(), len(nullable_fields))
        for field in nullable_fields:
            with self.subTest(field=field):
                field_value = '' if isinstance(field, FileField) else None
                self.assertEqual(NullableFields.objects.filter(**{field.name: field_value}).count(), 1)

    @skipUnlessDBFeature('can_return_rows_from_bulk_insert')
    def test_set_pk_and_insert_single_item(self):
        with self.assertNumQueries(1):
            countries = Country.objects.bulk_create([self.data[0]])
        self.assertEqual(len(countries), 1)
        self.assertEqual(Country.objects.get(pk=countries[0].pk), countries[0])

    @skipUnlessDBFeature('can_return_rows_from_bulk_insert')
    def test_set_pk_and_query_efficiency(self):
        with self.assertNumQueries(1):
            countries = Country.objects.bulk_create(self.data)
        self.assertEqual(len(countries), 4)
        self.assertEqual(Country.objects.get(pk=countries[0].pk), countries[0])
        self.assertEqual(Country.objects.get(pk=countries[1].pk), countries[1])
        self.assertEqual(Country.objects.get(pk=countries[2].pk), countries[2])
        self.assertEqual(Country.objects.get(pk=countries[3].pk), countries[3])

    @skipUnlessDBFeature('can_return_rows_from_bulk_insert')
    def test_set_state(self):
        country_nl = Country(name='Netherlands', iso_two_letter='NL')
        country_be = Country(name='Belgium', iso_two_letter='BE')
        Country.objects.bulk_create([country_nl])
        country_be.save()
        # Objects save via bulk_create() and save() should have equal state.
        self.assertEqual(country_nl._state.adding, country_be._state.adding)
        self.assertEqual(country_nl._state.db, country_be._state.db)

    def test_set_state_with_pk_specified(self):
        state_ca = State(two_letter_code='CA')
        state_ny = State(two_letter_code='NY')
        State.objects.bulk_create([state_ca])
        state_ny.save()
        # Objects save via bulk_create() and save() should have equal state.
        self.assertEqual(state_ca._state.adding, state_ny._state.adding)
        self.assertEqual(state_ca._state.db, state_ny._state.db)

    @skipIfDBFeature('supports_ignore_conflicts')
    def test_ignore_conflicts_value_error(self):
        message = 'This database backend does not support ignore conflicts.'
        with self.assertRaisesMessage(NotSupportedError, message):
            TwoFields.objects.bulk_create(self.data, ignore_conflicts=True)

    @skipUnlessDBFeature('supports_ignore_conflicts')
    def test_ignore_conflicts_ignore(self):
        data = [
            TwoFields(f1=1, f2=1),
            TwoFields(f1=2, f2=2),
            TwoFields(f1=3, f2=3),
        ]
        TwoFields.objects.bulk_create(data)
        self.assertEqual(TwoFields.objects.count(), 3)
        # With ignore_conflicts=True, conflicts are ignored.
        conflicting_objects = [
            TwoFields(f1=2, f2=2),
            TwoFields(f1=3, f2=3),
        ]
        TwoFields.objects.bulk_create([conflicting_objects[0]], ignore_conflicts=True)
        TwoFields.objects.bulk_create(conflicting_objects, ignore_conflicts=True)
        self.assertEqual(TwoFields.objects.count(), 3)
        self.assertIsNone(conflicting_objects[0].pk)
        self.assertIsNone(conflicting_objects[1].pk)
        # New objects are created and conflicts are ignored.
        new_object = TwoFields(f1=4, f2=4)
        TwoFields.objects.bulk_create(conflicting_objects + [new_object], ignore_conflicts=True)
        self.assertEqual(TwoFields.objects.count(), 4)
        self.assertIsNone(new_object.pk)
        # Without ignore_conflicts=True, there's a problem.
        with self.assertRaises(IntegrityError):
            TwoFields.objects.bulk_create(conflicting_objects)

    @skipIfDBFeature('supports_update_conflicts_with_unique_fields', 'supports_update_conflicts_without_unique_fields')
    def test_update_value_error(self):
        message = 'This database backend does not support update.'
        with self.assertRaisesMessage(NotSupportedError, message):
            TwoFields.objects.bulk_create(self.data, update_conflicts=True)

    def _test_update(self, **kwargs):
        data = [
            UpsertConflict(unique_field=1, integer_field=1, will_update=False),
            UpsertConflict(unique_field=2, integer_field=2, will_update=False),
            UpsertConflict(unique_field=3, integer_field=3, will_update=False),
        ]
        UpsertConflict.objects.bulk_create(data)
        self.assertEqual(UpsertConflict.objects.count(), 3)

        # With update_conflicts=True, conflicts are updated and determine by update_fields.
        update_objects_1 = [
            UpsertConflict(unique_field=2, integer_field=2, will_update=True),
            UpsertConflict(unique_field=3, integer_field=2, will_update=True),
        ]

        UpsertConflict.objects.bulk_create(
            update_objects_1, update_conflicts=True,
            update_fields=['integer_field', 'will_update'], **kwargs
        )
        self.assertEqual(UpsertConflict.objects.count(), 3)
        # if update, data will change.
        for obj in update_objects_1:
            self.assertIsNone(obj.pk)
            need_check = UpsertConflict.objects.get(unique_field=obj.unique_field)
            self.assertEqual(need_check.will_update, obj.will_update)
            self.assertEqual(need_check.integer_field, obj.integer_field)

        update_objects_2 = [
            UpsertConflict(unique_field=2, integer_field=5, will_update=True),
            UpsertConflict(unique_field=3, integer_field=5, will_update=True),
        ]
        UpsertConflict.objects.bulk_create(
            update_objects_2, update_conflicts=True,
            update_fields=['will_update'], **kwargs
        )
        self.assertEqual(UpsertConflict.objects.count(), 3)
        # if update, data will change.
        for obj in update_objects_2:
            self.assertIsNone(obj.pk)
            need_check = UpsertConflict.objects.get(unique_field=obj.unique_field)
            self.assertEqual(need_check.will_update, obj.will_update)
            # integer_field is not in update_fields
            self.assertEqual(need_check.integer_field, 2)

        # New objects are created and conflicts are ignored.
        new_object = UpsertConflict(unique_field=4, integer_field=4, will_update=False)
        update_objects_2 = [
            UpsertConflict(unique_field=2, integer_field=4, will_update=False),
            UpsertConflict(unique_field=3, integer_field=4, will_update=False),
        ]
        UpsertConflict.objects.bulk_create(
            update_objects_2 + [new_object], update_conflicts=True,
            update_fields=['integer_field', 'will_update'], **kwargs
        )
        self.assertEqual(UpsertConflict.objects.count(), 4)
        self.assertIsNone(new_object.pk)
        self.assertEqual(
            UpsertConflict.objects.get(unique_field=new_object.unique_field).will_update,
            new_object.will_update
        )
        self.assertEqual(
            UpsertConflict.objects.get(unique_field=new_object.unique_field).integer_field,
            new_object.integer_field
        )

        # if update, data will change.
        for obj in update_objects_2:
            self.assertIsNone(obj.pk)
            need_check = UpsertConflict.objects.get(unique_field=obj.unique_field)
            self.assertEqual(need_check.will_update, obj.will_update)
            self.assertEqual(need_check.integer_field, obj.integer_field)

    @skipUnlessDBFeature('supports_update_conflicts_without_unique_fields')
    def test_update__without_unique_fields(self):
        self._test_update()

    @skipUnlessDBFeature('supports_update_conflicts_with_unique_fields')
    def test_update__with_unique_fields(self):
        self._test_update(unique_fields=['unique_field'])

    def _test_update_together(self, **kwargs):
        data = [
            UniqueTogether(unique_together1=1, unique_together2=1, will_update=False),
            UniqueTogether(unique_together1=1, unique_together2=2, will_update=False),
            UniqueTogether(unique_together1=2, unique_together2=2, will_update=False),
        ]
        UniqueTogether.objects.bulk_create(data)
        self.assertEqual(UniqueTogether.objects.count(), 3)
        # With update_conflicts=True, conflicts are updated and determine by update_fields.
        new_objects = [
            UniqueTogether(unique_together1=1, unique_together2=3, will_update=True),
            UniqueTogether(unique_together1=2, unique_together2=3, will_update=True),
        ]
        update_objects_1 = [
            UniqueTogether(unique_together1=1, unique_together2=1, will_update=True),
            UniqueTogether(unique_together1=1, unique_together2=2, will_update=True),
            UniqueTogether(unique_together1=2, unique_together2=2, will_update=True),
        ]
        UniqueTogether.objects.bulk_create(
            new_objects, update_conflicts=True,
            update_fields=['will_update'], **kwargs
        )
        UniqueTogether.objects.bulk_create(
            update_objects_1, update_conflicts=True,
            update_fields=['will_update'], **kwargs
        )
        self.assertEqual(UniqueTogether.objects.count(), 5)
        # new objs
        for obj in new_objects:
            self.assertIsNone(obj.pk)
            need_check = UniqueTogether.objects.get(
                unique_together1=obj.unique_together1, unique_together2=obj.unique_together2
            )
            self.assertEqual(need_check.will_update, obj.will_update)

        # if update, data will change.
        for obj in update_objects_1:
            self.assertIsNone(obj.pk)
            need_check = UniqueTogether.objects.get(
                unique_together1=obj.unique_together1, unique_together2=obj.unique_together2
            )
            self.assertEqual(need_check.will_update, obj.will_update)

    @skipUnlessDBFeature('supports_update_conflicts_without_unique_fields')
    def test_update__unique_together__without_unique(self):
        self._test_update_together()

    @skipUnlessDBFeature('supports_update_conflicts_with_unique_fields')
    def test_update__unique_together__with_unique(self):
        self._test_update_together(unique_fields=['unique_together1', 'unique_together2'])

    @skipUnlessDBFeature('supports_update_conflicts_with_unique_fields')
    def test_update__unique_together__with_unique__error(self):
        msg = 'ON CONFLICT clause does not match any PRIMARY KEY or UNIQUE constraint'
        with self.assertRaisesMessage(OperationalError, msg):
            self._test_update_together(unique_fields=['unique_together1'])

    @skipUnlessDBFeature('supports_update_conflicts_without_unique_fields')
    def test_bulk_create__update_fields_error__without_unique(self):
        # Using update_conflict but without update_fields, there's a problem.
        msg = 'You need to specify which fields you want to update'
        with self.assertRaisesMessage(IntegrityError, msg):
            TwoFields.objects.bulk_create(self.data, update_conflicts=True)

        # Using update_conflict but without update_fields, there's a problem.
        field = 'test'
        msg = 'Field you specify is not in table: test'
        with self.assertRaisesMessage(IntegrityError, msg):
            TwoFields.objects.bulk_create(self.data, update_conflicts=True, update_fields=[field])

    @skipUnlessDBFeature('supports_update_conflicts_with_unique_fields')
    def test_bulk_create__update_fields_error__with_unique(self):
        # Using update_conflict but without update_fields, there's a problem.
        msg = 'You need to specify which fields you want to update'
        with self.assertRaisesMessage(IntegrityError, msg):
            TwoFields.objects.bulk_create(self.data, update_conflicts=True, unique_fields=['name'])

        # Using update_conflict but without update_fields, there's a problem.
        field = 'test'
        msg = 'Field you specify is not in table: test'
        with self.assertRaisesMessage(IntegrityError, msg):
            TwoFields.objects.bulk_create(
                self.data, update_conflicts=True, unique_fields=['name'], update_fields=[field]
            )

    @skipUnlessAnyDBFeature(
        'supports_ignore_conflicts', 'supports_update_conflicts_with_unique_fields',
        'supports_update_conflicts_without_unique_fields'
    )
    def test_bulk_create_on_conflicts_conflict(self):
        message = 'You can only assign one conflicts plan, ignore_conflicts or update_conflicts'
        with self.assertRaisesMessage(IntegrityError, message):
            TwoFields.objects.bulk_create(
                self.data,
                ignore_conflicts=True,
                update_conflicts=True
            )

    def test_nullable_fk_after_parent(self):
        parent = NoFields()
        child = NullableFields(auto_field=parent, integer_field=88)
        parent.save()
        NullableFields.objects.bulk_create([child])
        child = NullableFields.objects.get(integer_field=88)
        self.assertEqual(child.auto_field, parent)

    @skipUnlessDBFeature('can_return_rows_from_bulk_insert')
    def test_nullable_fk_after_parent_bulk_create(self):
        parent = NoFields()
        child = NullableFields(auto_field=parent, integer_field=88)
        NoFields.objects.bulk_create([parent])
        NullableFields.objects.bulk_create([child])
        child = NullableFields.objects.get(integer_field=88)
        self.assertEqual(child.auto_field, parent)

    def test_unsaved_parent(self):
        parent = NoFields()
        msg = (
            "bulk_create() prohibited to prevent data loss due to unsaved "
            "related object 'auto_field'."
        )
        with self.assertRaisesMessage(ValueError, msg):
            NullableFields.objects.bulk_create([NullableFields(auto_field=parent)])
